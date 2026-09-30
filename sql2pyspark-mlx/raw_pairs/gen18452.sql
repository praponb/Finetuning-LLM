select
    s.id,
    s.name,
    s.email,
    s.city,
    s.signup_date,
    s.is_active,
    s.credit_limit,
    x.order_status
from customers s
left join lateral (
        select c.status as order_status
        from orders c
        where c.customer_id = s.id
        order by c.shipping_fee desc, c.id desc
        limit 1
    ) x on true
