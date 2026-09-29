SELECT p.category, COUNT(ss.units) AS units_count, MAX(ss.sale_date) AS last_sale_date
FROM store_sales ss
LEFT OUTER JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
GROUP BY p.category
