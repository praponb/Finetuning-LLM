SELECT *
FROM (
    SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket, MAX(unit_price) - MIN(unit_price) AS unit_price_range
    FROM order_items
    WHERE unit_price BETWEEN 130 AND 210
    GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END
) AS agg
WHERE unit_price_range < 71
ORDER BY unit_price_range DESC;
