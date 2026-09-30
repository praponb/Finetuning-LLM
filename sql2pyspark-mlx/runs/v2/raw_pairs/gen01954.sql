SELECT p.supplier_id, ROUND(AVG(oi.quantity), 2) AS avg_quantity, MAX(oi.unit_price) - MIN(oi.unit_price) AS unit_price_range
FROM order_items oi
JOIN products p ON oi.product_id = p.id
LEFT OUTER JOIN orders o ON oi.order_id = o.id
GROUP BY p.supplier_id;
