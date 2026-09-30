SELECT p.brand, SUM(oi.quantity) AS total_quantity FROM order_items oi LEFT JOIN products p ON oi.product_id = p.id FULL OUTER JOIN suppliers s ON s.id = p.supplier_id GROUP BY p.brand;
