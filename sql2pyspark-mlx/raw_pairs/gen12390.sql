SELECT oi.order_id, p.category, p.product_name, p.cost FROM order_items oi LEFT JOIN products p ON oi.product_id = p.id
