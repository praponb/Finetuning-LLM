SELECT p.supplier_id, MAX(ss.units) AS max_units, COUNT(ss.units) AS units_count
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
GROUP BY p.supplier_id
ORDER BY max_units
