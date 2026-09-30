WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CAST(unit_price AS INT) AS unit_price_int
    FROM order_items
    WHERE quantity >= 8
)
SELECT product_id, id
FROM filtered
EXCEPT DISTINCT
SELECT product_id, id
FROM order_items
WHERE unit_price < 180;
