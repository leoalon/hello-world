with 
margen as (select 
if(t.pais_codigo in ('AR','CO','EC','PE','BR','CL','MX'),pais_codigo,'OT') as pais,
if(if((t.tipodecompra = 'Vuelos' and p.producto = 'Asistencia al viajero') or (t.tipodecompra in ('Bundles', 'Escapadas', 'Carrito') and p.etapaattach = 'CHECKOUT' and p.producto = 'Asistencia al viajero'), 'Asistencia al viajero', t.tipodecompra) in ('Bundles', 'Escapadas'),'Carrito', if((t.tipodecompra = 'Vuelos' and p.producto = 'Asistencia al viajero') or (t.tipodecompra in ('Bundles', 'Escapadas', 'Carrito') and p.etapaattach = 'CHECKOUT' and p.producto = 'Asistencia al viajero'), 'Asistencia al viajero', t.tipodecompra)) as productooriginal,
week(t.fechaconfirmacion) as semana,
p.tipoviaje as viaje,
sum(b.margenvariable_neto_usd+if(a. ufAA is null,0,a. ufAA)) as margenVAr_conAA,
sum(gb_withoutdistortedtaxes_usd) as gb,
count(distinct p.transaction_code) as bookings,
sum(b.comisiontotal_neto_usd+if(a. ufAA is null,0,a. ufAA)) as coimision,
sum(cant_pasajeros) as pasajeros,
sum(b.feetotal_neto_usd) as fee,
sum(-if(b.descuentostotal_neto_usd<0,0,b.descuentostotal_neto_usd)) as descuentos,
sum(-costomkt_neto_usd) as performance,
sum(-coixproducto_usd) as coi,
sum(ccpxproducto_usd) as ccp,
sum(-cancellations_usd) as cnl,
sum(otherincentivesair_usd) as Other_incentives,
sum(otros_usd) as otros_ccvv,
--sum(cross_sell*gb) as cross_sell,
--sum(desc_partner) as desc_partner,
--sum(customer_claims) as customer_claims,
--sum(breakage_revenue) as breakage_revenue,
sum(-customerservice_usd) as customer_service,
sum(-loyalty_usd) as loyalty_revenue,
sum(-afiliadas_usd) as comision_afiliadas,
sum(b.comisiontotal_usd+if(a. ufAA is null,0,a. ufAA)) as comision_bruta,
sum(b.feetotal_usd) as fee_bruto,
sum(-b.descuentostotal_usd) as descuentos_brutos,
sum(impuestocomision_usd) as impuesto_comision,
sum(impuestofee_usd) as impuestofee,
sum(impuestodescuento_usd) as impuesto_descuento,
-sum(b.frauds_usd) as frauds,
-sum(b.errors_usd) as errors,
-sum(b.revtaxes_usd) as revenue_tax,
-sum(b.ott_usd) as ott,
sum(if(funds.mktg_funds_usd is null, 0, funds.mktg_funds_usd)) as desc_partner
from 
data.lake.bi_PnLOp b 
join
data.lake.bi_productos p on cast(p.product_id as bigint)=b.product_id 
join data.lake.bi_transacciones t on t.transaction_code=p.transaction_code
left join  data.tmp.comision_AA a on a.product_id=b.product_id
left join data.tmp.mktg_funds funds on cast(funds.product_id as bigint) = b.product_id
where b.fechareserva>=date'2017-01-01' and 
p.prod_fechacorte>='2020' and t.trx_fechacorte>='2020'
and  week(t.fechaconfirmacion)<week(date_add('day',-dow(current_date)+1,current_date))
and year(t.fechaconfirmacion)=2020 and week(t.fechaconfirmacion)>=9 
and p.flg_confirmado=1 --and p.flg_emitido=1
and t.channel not like '%falabella%' 
group by 1,2,3,4),
effin as (
select
if(pais in ('AR','CO','EC','PE','BR','CL','MX'),pais,'OT') as pais,
semana,
productoOriginal,
viaje,
sum(resultado_financiero) as resultado_financiero
from data.tmp.resultado_financiero
where year(fecha)=2020 and semana>=9 and semana<week(date_add('day',-dow(current_date)+1,current_date)) 
group by 1,2,3,4
)
select 
m.semana,
m.productooriginal,
m.pais,
m.viaje,
m.gb, 
m.bookings,
m.coimision,
m.pasajeros,
m.fee,
m.descuentos,
m.performance,
m.coi,
m.ccp,
m.cnl,
m.Other_incentives,
m.otros_ccvv,
margenVAr_conAA+resultado_financiero+desc_partner as npv,
margenVAr_conAA+desc_partner as mgvar,
if(mg_extra_xsell is null,0,cast(mg_extra_xsell as real))*m.gb as cross_sell,
desc_partner,
--sum(customer_claims) as customer_claims,
--sum(breakage_revenue) as breakage_revenue,
m.customer_service,
m.loyalty_revenue,
m.comision_afiliadas,
m.comision_bruta,
m.fee_bruto,
m.descuentos_brutos,
m.impuesto_comision,
m.impuestofee,
m.impuesto_descuento,
errors,
frauds,
ott,
revenue_tax as revtaxes
from 
margen m 
left join effin e on e.viaje=m.viaje and e.pais=m.pais and e.productooriginal=m.productooriginal and e.semana=m.semana  
left join data.tmp.extra_mg_xsell xs on xs.productooriginal=m.ProductoOriginal and xs.Viaje=m.Viaje and m.pais=xs.pais 
where (margenVAr_conAA+resultado_financiero)/m.gb is not null