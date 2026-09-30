select p.id, p.status, o.discount
from payments p
inner join orders o on o.id = p.order_id
where o.amount between 520 and 820;
