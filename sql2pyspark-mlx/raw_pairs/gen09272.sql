WITH filtered AS (
    SELECT *, unit_price - quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
)
SELECT
    id,
    order_id,
    quantity,
    unit_price,
    line_total
FROM filtered
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY quantity DESC, id) = 1
