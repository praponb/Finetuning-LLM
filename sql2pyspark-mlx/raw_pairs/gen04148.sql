select
    o.id,
    o.amount,
    o.store_id,
    s.region,
    s.city,
    c.email
from orders o
left join stores s on s.id = o.store_id
left join customers c on c.id = o.customer_id
where o.amount between 1280 and 1350 and o.order_date < '2025-05-01'
