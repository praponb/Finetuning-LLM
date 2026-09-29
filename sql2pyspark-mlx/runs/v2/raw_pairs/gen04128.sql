SELECT p.brand, COUNT(oi.quantity) quantity_count, ROUND(STDDEV(oi.unit_price), 2) stddev_unit_price
FROM order_items oi
RIGHT JOIN orders o ON o.id = oi.order_id
RIGHT JOIN products p ON p.id = oi.product_id
WHERE p.price > 730
GROUP BY p.brand
ORDER BY quantity_count DESC
