select
    p.id,
    p.paid_at,
    p.amount,
    p.status,
    o.status as order_status,
    o.order_date
from payments p
join orders o on p.order_id = o.id
where o.discount < 0.33;
