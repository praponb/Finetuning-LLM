select month(p.launch_date) as launch_month, max(oi.quantity) as max_quantity
from order_items oi
inner join products p on oi.product_id = p.id
inner join orders o on oi.order_id = o.id
where o.status != 'SHIPPED'
group by month(p.launch_date)
order by max_quantity asc
