SELECT CASE WHEN units >= 43 THEN 'high' WHEN units >= 16 THEN 'mid' ELSE 'low' END AS units_bucket, MIN(sale_date) AS min_sale_date, MAX(units) - MIN(units) AS units_range
FROM store_sales
GROUP BY CASE WHEN units >= 43 THEN 'high' WHEN units >= 16 THEN 'mid' ELSE 'low' END
ORDER BY min_sale_date;
