SELECT p.category, SUM(oi.unit_price) AS total_unit_price FROM order_items oi INNER JOIN products p ON p.id = oi.product_id GROUP BY p.category ORDER BY p.category ASC LIMIT 5;
