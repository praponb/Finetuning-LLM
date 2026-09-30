select
    p.id,
    p.paid_at,
    o.status,
    o.order_date
from payments p
right join orders o on p.order_id = o.id
order by o.status, id
