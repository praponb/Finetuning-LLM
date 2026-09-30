SELECT p.category, MIN(oi.quantity) AS min_quantity, SUM(oi.quantity) AS total_quantity FROM order_items oi LEFT JOIN products p ON oi.product_id = p.id GROUP BY p.category
