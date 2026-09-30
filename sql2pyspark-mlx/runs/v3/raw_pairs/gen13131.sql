SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket, MAX(unit_price) - MIN(unit_price) AS unit_price_range
FROM order_items
WHERE unit_price > 230
GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END
HAVING MAX(unit_price) - MIN(unit_price) > 33
ORDER BY quantity_bucket
LIMIT 5;
