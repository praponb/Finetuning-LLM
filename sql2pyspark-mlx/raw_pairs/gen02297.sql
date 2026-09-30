SELECT s.city, COUNT(o.shipping_fee) AS shipping_fee_count, COUNT(DISTINCT o.shipping_fee) AS unique_customers
FROM orders o
LEFT JOIN stores s ON s.id = o.store_id
JOIN customers c ON o.customer_id = c.id
WHERE o.shipping_fee BETWEEN 19 AND 23
GROUP BY s.city;
