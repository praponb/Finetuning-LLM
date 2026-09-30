select p.id, p.amount, o.customer_id
from payments p
join orders o on p.order_id = o.id
where o.channel != 'store' and p.amount >= 1320;
