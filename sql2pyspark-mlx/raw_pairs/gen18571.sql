SELECT s.city, MAX(ss.units) AS max_units, MAX(ss.sale_date) AS last_sale_date
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
LEFT JOIN products p ON ss.product_id = p.id
WHERE ss.product_id = 6
GROUP BY s.city
