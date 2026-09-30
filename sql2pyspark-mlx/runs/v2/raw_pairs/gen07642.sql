select
    s.id,
    s.order_id,
    s.shipped_date,
    s.carrier
from shipments s
where s.order_id in (select id from orders where store_id in (9, 12, 31, 35))
