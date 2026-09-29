SELECT store_id, CASE WHEN units >= 31 THEN 'high' WHEN units >= 20 THEN 'mid' ELSE 'low' END AS units_bucket, MIN(units) AS min_units
FROM store_sales
WHERE units = 11
GROUP BY ROLLUP(store_id, CASE WHEN units >= 31 THEN 'high' WHEN units >= 20 THEN 'mid' ELSE 'low' END)
ORDER BY units_bucket;
