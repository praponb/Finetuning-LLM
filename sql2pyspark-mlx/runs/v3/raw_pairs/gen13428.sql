SELECT units_bucket, num_store_sales, units_range
FROM (
    SELECT CASE WHEN units >= 41 THEN 'high' WHEN units >= 20 THEN 'mid' ELSE 'low' END AS units_bucket, COUNT(*) AS num_store_sales, MAX(units) - MIN(units) AS units_range
    FROM store_sales
    GROUP BY CASE WHEN units >= 41 THEN 'high' WHEN units >= 20 THEN 'mid' ELSE 'low' END
) AS t
WHERE units_range < 22;
