SELECT CASE WHEN units >= 34 THEN 'high' WHEN units >= 10 THEN 'mid' ELSE 'low' END AS units_bucket, product_id, COUNT(DISTINCT revenue) AS unique_products
FROM store_sales
GROUP BY CASE WHEN units >= 34 THEN 'high' WHEN units >= 10 THEN 'mid' ELSE 'low' END, product_id
ORDER BY 1 DESC;
