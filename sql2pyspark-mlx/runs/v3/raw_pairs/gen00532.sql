select
    o.amount,
    o.shipping_fee,
    p.status,
    p.amount as payment_amount,
    p.paid_at
from orders o
left join payments p on o.id = p.order_id
where p.method in ('paypal', 'card') and p.status not in ('SUCCESS', 'FAILED')
order by amount desc;
