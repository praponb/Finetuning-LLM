select
    o.id,
    o.status,
    o.amount,
    s.store_name,
    c.country,
    c.credit_limit
from orders o
inner join stores s on o.store_id = s.id
inner join customers c on o.customer_id = c.id
where c.credit_limit >= 13900 and s.store_name like 'Ri%';
