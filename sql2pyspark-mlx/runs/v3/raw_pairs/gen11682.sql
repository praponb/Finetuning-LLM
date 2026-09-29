SELECT MIN(revenue) AS min_revenue
FROM store_sales
WHERE store_id IN (3, 4, 6);
