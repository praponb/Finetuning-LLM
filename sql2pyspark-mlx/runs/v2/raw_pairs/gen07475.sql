SELECT p.supplier_id, ROUND(STDDEV(oi.quantity), 2) AS stddev_quantity
FROM order_items oi
INNER JOIN products p ON p.id = oi.product_id
FULL OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.brand = 'Acme'
GROUP BY p.supplier_id
ORDER BY stddev_quantity DESC;
