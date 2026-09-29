select
    p.id,
    p.amount,
    p.status,
    o.order_date,
    cast(p.amount as int) amount_int
from payments p
right join orders o on p.order_id = o.id
order by order_date asc;
