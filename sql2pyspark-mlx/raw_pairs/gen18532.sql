select
    s.id,
    s.cost,
    s.delivered_date,
    s.shipped_date
from shipments s
where s.order_id not in (select id from orders where shipping_fee <= 26)
order by shipped_date, id
limit 10
