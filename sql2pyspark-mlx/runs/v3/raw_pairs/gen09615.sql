SELECT p.brand, ROUND(AVG(oi.quantity), 2) AS avg_quantity, ROUND(STDDEV(oi.unit_price), 2) AS stddev_unit_price
FROM order_items oi
RIGHT JOIN products p ON p.id = oi.product_id
WHERE oi.unit_price > 220
GROUP BY p.brand
ORDER BY brand DESC
LIMIT 50
