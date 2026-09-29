SELECT CASE WHEN units >= 43 THEN 'high' WHEN units >= 9 THEN 'mid' ELSE 'low' END AS units_bucket, product_id, MIN(sale_date) AS min_sale_date
FROM store_sales
WHERE product_id IN (2, 9, 10)
GROUP BY CASE WHEN units >= 43 THEN 'high' WHEN units >= 9 THEN 'mid' ELSE 'low' END, product_id
