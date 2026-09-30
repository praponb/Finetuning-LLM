SELECT p.brand, COUNT(ss.units) units_count, MIN(ss.units) min_units
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
WHERE p.launch_date < '2024-10-01'
GROUP BY p.brand;
