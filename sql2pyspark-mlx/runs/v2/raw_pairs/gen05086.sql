SELECT DATE_FORMAT(products.launch_date, 'yyyy-MM') AS launch_month, COUNT(DISTINCT store_sales.units) AS unique_stores
FROM store_sales
INNER JOIN stores ON store_sales.store_id = stores.id
LEFT JOIN products ON store_sales.product_id = products.id
WHERE store_sales.revenue >= 2900
GROUP BY DATE_FORMAT(products.launch_date, 'yyyy-MM')
