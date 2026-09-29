select o.id, o.order_date, o.shipping_fee
from orders o
where o.store_id in (11, 19) and o.customer_id not in (select id from customers where country != 'US')
order by order_date desc, id desc
