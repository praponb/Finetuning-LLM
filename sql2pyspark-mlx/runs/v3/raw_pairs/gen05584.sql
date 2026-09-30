SELECT *
FROM (
    SELECT order_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range
    FROM order_items
    WHERE product_id IS NULL
    GROUP BY order_id
) AS x
WHERE unit_price_range >= 53;
