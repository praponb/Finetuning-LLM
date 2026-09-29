WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        quantity - unit_price AS line_total,
        unit_price + quantity AS line_total_2
    FROM order_items
    WHERE product_id IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    order_id,
    metric,
    metric_value
FROM (
    SELECT
        id,
        order_id,
        line_total,
        unit_price,
        line_total_2
    FROM filtered
) src
UNPIVOT (
    metric_value FOR metric IN (line_total AS LINE_TOTAL, unit_price, line_total_2 AS LINE_TOTAL_2)
) unp;
