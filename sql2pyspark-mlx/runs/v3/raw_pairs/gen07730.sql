select p.paid_at, o.customer_id, o.order_date
from payments p
inner join orders o on p.order_id = o.id
where o.customer_id in (23, 25, 29, 35)
order by o.order_date desc;
