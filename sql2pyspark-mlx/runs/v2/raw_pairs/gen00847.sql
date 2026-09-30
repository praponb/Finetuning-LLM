SELECT oi.id, oi.order_id, oi.unit_price, o.order_date, o.customer_id, o.channel FROM order_items oi FULL OUTER JOIN orders o ON oi.order_id = o.id
