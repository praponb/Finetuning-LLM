select
    p.id,
    p.order_id,
    p.method,
    o.store_id,
    c.city,
    c.email
from payments p
inner join orders o on o.id = p.order_id
inner join customers c on o.customer_id = c.id
order by city asc;
