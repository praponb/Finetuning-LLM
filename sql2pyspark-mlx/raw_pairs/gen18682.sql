select
    p.order_id,
    p.paid_at,
    o.customer_id,
    c.name
from payments p
inner join orders o on p.order_id = o.id
left join customers c on c.id = o.customer_id
where p.amount >= 780;
