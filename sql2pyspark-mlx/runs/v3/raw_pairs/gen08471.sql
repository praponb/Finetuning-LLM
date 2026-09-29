select s.id, s.status, s.carrier
from shipments s
where s.order_id in (select id from orders where status not in ('SHIPPED', 'CANCELLED'));
