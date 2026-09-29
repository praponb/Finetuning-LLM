SELECT p.brand, AVG(oi.unit_price) AS avg_unit_price
FROM order_items oi
FULL OUTER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON p.id = oi.product_id
WHERE o.customer_id IS NOT NULL
GROUP BY p.brand
ORDER BY avg_unit_price DESC, brand
LIMIT 20;
