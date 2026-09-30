SELECT p.supplier_id, ROUND(STDDEV(ss.units), 2) AS stddev_units, MAX(ss.units) AS max_units
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
FULL OUTER JOIN suppliers s ON s.id = p.supplier_id
GROUP BY p.supplier_id
ORDER BY max_units DESC, stddev_units, supplier_id
LIMIT 3
