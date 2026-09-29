SELECT s.city, COUNT(ss.units) AS units_count
FROM store_sales ss
LEFT JOIN products p ON p.id = ss.product_id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE ss.sale_date BETWEEN '2023-07-01' AND '2023-11-01'
GROUP BY s.city
HAVING COUNT(ss.units) > 1
ORDER BY units_count DESC, s.city DESC;
