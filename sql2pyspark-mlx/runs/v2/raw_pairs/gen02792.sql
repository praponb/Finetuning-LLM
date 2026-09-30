SELECT p.brand, ROUND(STDDEV(oi.unit_price), 2) AS stddev_unit_price, MIN(oi.unit_price) AS min_unit_price
FROM order_items oi
LEFT OUTER JOIN orders o ON o.id = oi.order_id
LEFT OUTER JOIN products p ON p.id = oi.product_id
GROUP BY p.brand
ORDER BY brand
LIMIT 5;
