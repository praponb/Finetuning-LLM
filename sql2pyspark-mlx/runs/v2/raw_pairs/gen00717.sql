SELECT *
FROM (
    SELECT order_id, CASE WHEN unit_price >= 450 THEN 'high' WHEN unit_price >= 120 THEN 'mid' ELSE 'low' END unit_price_bucket, SUM(quantity) total_quantity
    FROM order_items
    WHERE quantity >= 7
    GROUP BY order_id, CASE WHEN unit_price >= 450 THEN 'high' WHEN unit_price >= 120 THEN 'mid' ELSE 'low' END
) sub
WHERE total_quantity < 34
