select
    p.status,
    o.store_id,
    o.discount,
    c.name,
    c.age
from payments p
right outer join orders o on o.id = p.order_id
left outer join customers c on o.customer_id = c.id
order by discount, age desc;
