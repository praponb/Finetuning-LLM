SELECT o.status, AVG(oi.quantity) avg_quantity, ROUND(STDDEV(oi.unit_price), 2) stddev_unit_price
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON o.id = oi.order_id
GROUP BY o.status
ORDER BY status ASC, stddev_unit_price ASC;
