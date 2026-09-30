SELECT p.category, MAX(oi.quantity) AS max_quantity, ROUND(STDDEV(oi.quantity), 2) AS stddev_quantity
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE s.supplier_name IS NOT NULL
GROUP BY p.category
HAVING stddev_quantity > 70;
