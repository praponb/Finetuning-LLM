select
    p.id,
    p.status,
    p.paid_at,
    p.amount,
    o.store_id,
    o.discount,
    o.order_date,
    round(p.amount * 1.1, 2) as amount_scaled
from payments p
right join orders o on p.order_id = o.id
order by 3;
