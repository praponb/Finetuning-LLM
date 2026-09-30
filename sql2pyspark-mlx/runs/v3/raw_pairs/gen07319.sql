select
    c.segment,
    c.credit_limit,
    c.email,
    o.discount,
    o.store_id
from customers c
left join orders o on o.customer_id = c.id
where c.signup_date >= '2025-05-01'
order by c.segment
