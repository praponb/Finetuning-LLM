select s.id, s.carrier
from shipments s
where s.order_id not in (select id from orders where store_id in (25, 35, 40));
