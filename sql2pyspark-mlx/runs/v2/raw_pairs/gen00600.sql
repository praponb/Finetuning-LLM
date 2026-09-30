select order_items.unit_price, orders.amount, orders.discount
from order_items
left outer join orders on orders.id = order_items.order_id
where order_items.product_id in (11, 21, 28)
order by unit_price
