select
    o.id,
    o.channel,
    c.credit_limit,
    c.name,
    s.store_name,
    s.city
from orders o
inner join customers c on c.id = o.customer_id
right join stores s on o.store_id = s.id;
