select o.id, o.shipping_fee
from orders o
where o.id in (select order_id from payments where to_date(paid_at) >= '2023-05-01');
