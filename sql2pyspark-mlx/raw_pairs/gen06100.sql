SELECT p.category, p.brand, COUNT(oi.unit_price) AS unit_price_count FROM order_items oi INNER JOIN products p ON oi.product_id = p.id GROUP BY p.category, p.brand;
