WITH filtered AS (
    SELECT
        *,
        unit_price + quantity line_total,
        GREATEST(quantity, unit_price) greatest_quantity_unit_price
    FROM order_items
    WHERE product_id IS NOT NULL
      AND quantity BETWEEN 4 AND 7
)
SELECT
    id,
    product_id,
    metric_name,
    reading
FROM (
    SELECT
        id,
        product_id,
        unit_price,
        line_total
    FROM filtered
) src
UNPIVOT (
    reading FOR metric_name IN (unit_price AS UNIT_PRICE, line_total AS LINE_TOTAL_VAL)
);
