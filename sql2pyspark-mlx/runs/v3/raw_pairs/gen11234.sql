select
    o.id,
    o.discount,
    o.shipping_fee,
    s.city,
    c.city customer_city
from orders o
right join stores s on s.id = o.store_id
inner join customers c on c.id = o.customer_id
order by o.shipping_fee desc
