select o.id, o.discount
from orders o
where not exists (select 1 from payments p where p.order_id = o.id and hour(p.paid_at) between 9 and 17)
