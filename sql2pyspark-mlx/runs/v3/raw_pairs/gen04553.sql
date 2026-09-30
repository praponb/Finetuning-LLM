SELECT unit_price_bucket, unit_price_range, unique_products
FROM (
    SELECT CASE WHEN unit_price >= 350 THEN 'high' WHEN unit_price >= 110 THEN 'mid' ELSE 'low' END AS unit_price_bucket, MAX(unit_price) - MIN(unit_price) AS unit_price_range, COUNT(DISTINCT unit_price) AS unique_products
    FROM order_items
    WHERE product_id IN (2, 29, 34)
    GROUP BY CASE WHEN unit_price >= 350 THEN 'high' WHEN unit_price >= 110 THEN 'mid' ELSE 'low' END
) AS x
WHERE unit_price_range < 26
