select
    p.id,
    p.order_id,
    o.order_date,
    o.amount,
    o.discount
from payments p
left join orders o on p.order_id = o.id;
