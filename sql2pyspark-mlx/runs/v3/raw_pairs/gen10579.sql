SELECT store_id, revenue_bucket, stddev_units
FROM (
    SELECT store_id, CASE WHEN revenue >= 3600 THEN 'high' WHEN revenue >= 1000 THEN 'mid' ELSE 'low' END AS revenue_bucket, ROUND(STDDEV(units), 2) AS stddev_units
    FROM store_sales
    GROUP BY store_id, CASE WHEN revenue >= 3600 THEN 'high' WHEN revenue >= 1000 THEN 'mid' ELSE 'low' END
) AS sub
WHERE stddev_units >= 23
ORDER BY stddev_units DESC;
