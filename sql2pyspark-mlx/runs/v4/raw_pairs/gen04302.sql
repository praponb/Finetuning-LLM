select
    p.id,
    p.paid_at,
    p.status,
    o.shipping_fee,
    o.amount,
    o.customer_id
from payments p
left outer join orders o on p.order_id = o.id
where p.status in ('SUCCESS', 'FAILED', 'REFUNDED') and o.amount <= 1050;
