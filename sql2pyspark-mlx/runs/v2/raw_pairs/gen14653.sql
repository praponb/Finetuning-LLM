select
    s.id,
    s.delivered_date,
    s.order_id,
    s.carrier
from shipments s
where s.order_id = 6 and s.order_id not in (select id from orders where status in ('CANCELLED', 'PENDING'));
