select
    s.id,
    s.delivered_date,
    o.customer_id,
    o.channel
from shipments s
left join orders o on o.id = s.order_id
where s.carrier = 'DHL' and s.carrier = 'UPS'
order by s.id desc, delivered_date asc
limit 50;
