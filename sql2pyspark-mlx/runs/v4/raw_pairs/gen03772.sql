select s.id, s.status, s.carrier
from shipments s
where s.status in ('DELIVERED', 'IN_TRANSIT') and s.order_id in (select id from orders where amount <= 1030)
