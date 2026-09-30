select o.id, o.amount, o.discount
from orders o
where o.amount is null and exists (select 1 from payments p where p.order_id = o.id)
order by discount asc, amount desc;
