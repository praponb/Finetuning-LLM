SELECT CASE WHEN units >= 43 THEN 'high' WHEN units >= 13 THEN 'mid' ELSE 'low' END AS units_bucket, store_id, ROUND(STDDEV(units), 2) AS stddev_units
FROM store_sales
WHERE sale_date > '2024-08-01'
GROUP BY CASE WHEN units >= 43 THEN 'high' WHEN units >= 13 THEN 'mid' ELSE 'low' END, store_id
ORDER BY units_bucket DESC, store_id
LIMIT 10;
