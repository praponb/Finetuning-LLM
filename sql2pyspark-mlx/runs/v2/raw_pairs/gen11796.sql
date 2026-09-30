select
    o.id,
    o.shipping_fee,
    o.store_id,
    o.status
from orders o
where o.customer_id in (select id from customers where country != 'DE')
order by o.store_id desc, o.shipping_fee desc, id
limit 10
