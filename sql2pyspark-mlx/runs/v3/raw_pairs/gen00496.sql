select
    p.id,
    p.order_id,
    p.status,
    o.amount,
    o.discount
from payments p
inner join orders o on o.id = p.order_id
where o.customer_id is null
order by discount desc;
