WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price * quantity AS line_total,
        quantity + unit_price AS line_total_2
    FROM order_items
    WHERE product_id IN (12, 14)
)
SELECT product_id
FROM valid
INTERSECT ALL
SELECT product_id
FROM order_items
WHERE order_id IN (2, 4, 24)
