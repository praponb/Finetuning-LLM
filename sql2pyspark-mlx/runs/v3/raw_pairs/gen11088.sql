SELECT *
FROM (
    SELECT CASE WHEN unit_price >= 390 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END unit_price_bucket, MAX(quantity) max_quantity
    FROM order_items
    GROUP BY CASE WHEN unit_price >= 390 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END
) sub
WHERE max_quantity >= 8.1;
