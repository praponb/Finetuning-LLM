SELECT unit_price_bucket, quantity_count
FROM (
    SELECT CASE WHEN unit_price >= 320 THEN 'high' WHEN unit_price >= 180 THEN 'mid' ELSE 'low' END AS unit_price_bucket, COUNT(quantity) AS quantity_count
    FROM order_items
    WHERE unit_price IS NOT NULL
    GROUP BY CASE WHEN unit_price >= 320 THEN 'high' WHEN unit_price >= 180 THEN 'mid' ELSE 'low' END
) AS sub
WHERE quantity_count < 4
ORDER BY unit_price_bucket ASC, quantity_count ASC
LIMIT 5
