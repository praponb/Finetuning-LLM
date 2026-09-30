select p.status, o.discount, o.order_date
from payments p
left outer join orders o on p.order_id = o.id
order by 3 asc, p.status asc;
