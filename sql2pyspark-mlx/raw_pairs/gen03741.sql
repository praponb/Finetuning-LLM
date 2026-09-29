SELECT p.brand, MAX(ss.sale_date) AS last_sale_date, COUNT(DISTINCT ss.units) AS unique_products
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
GROUP BY p.brand
HAVING unique_products >= 5
ORDER BY unique_products DESC
