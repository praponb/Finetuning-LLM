SELECT s.city, SUM(oi.quantity) AS total_quantity, COUNT(*) AS row_count
FROM order_items oi
JOIN products p ON p.id = oi.product_id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE oi.unit_price > 270
GROUP BY s.city;
