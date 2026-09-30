SELECT *
FROM (
    SELECT product_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range
    FROM order_items
    GROUP BY product_id
) AS x
WHERE unit_price_range > 36
ORDER BY product_id
LIMIT 5
