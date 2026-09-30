select
    c.city,
    c.signup_date,
    c.country,
    o.discount,
    o.status,
    o.shipping_fee
from customers c
left join orders o on c.id = o.customer_id
where o.discount > 0.27;
