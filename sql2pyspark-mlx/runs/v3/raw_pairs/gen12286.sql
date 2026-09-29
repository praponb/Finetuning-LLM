select
    p.id,
    p.order_id,
    p.paid_at,
    o.amount,
    c.signup_date
from payments p
inner join orders o on p.order_id = o.id
inner join customers c on c.id = o.customer_id
