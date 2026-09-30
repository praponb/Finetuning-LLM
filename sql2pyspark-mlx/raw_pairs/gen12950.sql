SELECT unit_price_bucket, max_quantity, total_quantity
FROM (
    SELECT CASE WHEN unit_price >= 320 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END AS unit_price_bucket, MAX(quantity) AS max_quantity, SUM(quantity) AS total_quantity
    FROM order_items
    WHERE order_id IN (2, 36)
    GROUP BY CASE WHEN unit_price >= 320 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END
) AS t
WHERE total_quantity > 40;
