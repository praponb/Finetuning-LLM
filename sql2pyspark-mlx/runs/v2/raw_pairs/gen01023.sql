WITH valid_order_items AS (
    SELECT *, unit_price * quantity AS line_total
    FROM order_items
    WHERE order_id IN (1, 27, 31, 40)
),
Summary AS (
    SELECT
        order_id,
        product_id,
        MAX(line_total) AS max_line_total,
        ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM valid_order_items
    GROUP BY order_id, product_id
)
SELECT *
FROM Summary
