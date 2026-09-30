SELECT ss.product_id, YEAR(ss.sale_date) AS sale_year, MIN(i.last_restock) AS first_last_restock
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.sale_date < '2024-05-01'
GROUP BY ss.product_id, YEAR(ss.sale_date);
