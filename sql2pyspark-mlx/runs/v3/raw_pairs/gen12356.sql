SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(unit_price) AS unit_price_count, MIN(unit_price) AS min_unit_price
FROM order_items
WHERE unit_price < 260
GROUP BY 1
ORDER BY unit_price_count;
