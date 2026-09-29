SELECT s.city, COUNT(oi.unit_price) AS unit_price_count, SUM(oi.quantity * oi.unit_price) AS revenue
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.supplier_id = 36
GROUP BY s.city;
