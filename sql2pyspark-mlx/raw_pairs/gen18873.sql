select orders.customer_id, count(distinct order_items.quantity) unique_orders, count(order_items.unit_price) unit_price_count
from order_items
inner join products on order_items.product_id = products.id
left join orders on order_items.order_id = orders.id
where array_contains(products.tags, 'clearance')
group by orders.customer_id
order by customer_id desc, unique_orders
