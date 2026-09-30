select
    p.id,
    p.paid_at,
    p.amount,
    o.order_date
from payments p
left join orders o on o.id = p.order_id
where o.store_id in (17, 29);
