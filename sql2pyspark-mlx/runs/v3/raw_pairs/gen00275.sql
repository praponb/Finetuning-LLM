SELECT p.brand, AVG(ss.units) avg_units
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
JOIN stores s ON ss.store_id = s.id
GROUP BY p.brand
