SELECT *
FROM (
    SELECT product_id, MIN(unit_price) AS min_unit_price
    FROM order_items
    GROUP BY product_id
) AS agg
WHERE min_unit_price > 180
ORDER BY min_unit_price, product_id
LIMIT 1;
