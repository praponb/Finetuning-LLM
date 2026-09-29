SELECT o.status, MIN(oi.unit_price) AS min_unit_price, ROUND(STDDEV(oi.quantity), 2) AS stddev_quantity
FROM order_items oi
LEFT OUTER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id
GROUP BY o.status
ORDER BY min_unit_price DESC;
