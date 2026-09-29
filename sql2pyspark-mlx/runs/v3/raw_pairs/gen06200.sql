SELECT *
FROM (
    SELECT CASE WHEN units >= 40 THEN 'high' WHEN units >= 11 THEN 'mid' ELSE 'low' END units_bucket, MIN(units) min_units
    FROM store_sales
    GROUP BY CASE WHEN units >= 40 THEN 'high' WHEN units >= 11 THEN 'mid' ELSE 'low' END
) x
WHERE min_units > 16
ORDER BY units_bucket DESC, min_units
