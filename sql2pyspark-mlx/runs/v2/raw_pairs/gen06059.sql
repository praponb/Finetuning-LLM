SELECT *
FROM (
    SELECT CASE WHEN unit_price >= 430 THEN 'high' WHEN unit_price >= 120 THEN 'mid' ELSE 'low' END AS unit_price_bucket, SUM(quantity) AS total_quantity, COUNT(DISTINCT unit_price) AS unique_orders
    FROM order_items
    GROUP BY CASE WHEN unit_price >= 430 THEN 'high' WHEN unit_price >= 120 THEN 'mid' ELSE 'low' END
) AS s
WHERE unique_orders < 6
