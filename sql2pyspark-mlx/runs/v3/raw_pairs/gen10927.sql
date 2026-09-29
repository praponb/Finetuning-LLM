select
    p.amount,
    p.order_id,
    o.channel,
    o.order_date,
    o.status
from payments p
left join orders o on o.id = p.order_id
where p.paid_at < '2024-04-01' and p.amount between 520 and 1380;
