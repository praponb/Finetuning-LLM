select p.amount, o.discount
from payments p
join orders o on p.order_id = o.id
where o.amount between 650 and 1460 and p.status in ('SUCCESS', 'FAILED')
order by amount desc, o.discount desc;
