select o.id, o.status, o.shipping_fee
from orders o
where o.order_date < '2024-10-01' and not exists (select 1 from shipments s where s.order_id = o.id);
