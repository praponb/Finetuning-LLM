SELECT p.brand, SUM(oi.quantity) AS total_quantity FROM order_items oi LEFT OUTER JOIN products p ON oi.product_id = p.id GROUP BY p.brand;
