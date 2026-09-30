select o.amount, oi.order_id from orders o join order_items oi on o.id = oi.order_id where oi.order_id in (24, 28) and oi.product_id in (17, 27)
