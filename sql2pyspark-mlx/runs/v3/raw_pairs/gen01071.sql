select o.customer_id, month(o.order_date) as order_month, count(*) as total_count
from shipments s
join orders o on o.id = s.order_id
where o.status in ('CANCELLED', 'PENDING', 'SHIPPED')
group by o.customer_id, month(o.order_date)
