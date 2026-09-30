SELECT p.supplier_id, AVG(i.quantity) AS avg_quantity, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
GROUP BY p.supplier_id
