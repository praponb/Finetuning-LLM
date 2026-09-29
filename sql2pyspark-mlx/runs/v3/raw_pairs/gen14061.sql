SELECT oi.unit_price, oi.order_id, oi.product_id, p.launch_date, p.product_name, p.category FROM order_items oi LEFT JOIN products p ON oi.product_id = p.id
