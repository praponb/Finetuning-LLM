select
    o.id,
    o.customer_id,
    c.name,
    c.signup_date,
    s.opened_date,
    s.region
from orders o
join customers c on o.customer_id = c.id
join stores s on o.store_id = s.id
where o.customer_id in (14, 22) and length(s.store_name) < 10
order by o.id, c.name
