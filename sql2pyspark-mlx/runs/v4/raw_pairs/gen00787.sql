select o.id, o.discount, o.amount
from orders o
where o.channel = 'web' and not exists (select 1 from payments p where p.order_id = o.id)
