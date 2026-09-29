select orders.status, orders.channel, count(distinct order_items.quantity) as unique_orders
from order_items
left outer join orders on orders.id = order_items.order_id
left outer join products on order_items.product_id = products.id
where order_items.quantity between 5 and 8
group by orders.status, orders.channel
order by orders.status desc;
