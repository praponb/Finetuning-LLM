WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        CAST(quantity AS DOUBLE) AS quantity_str
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (20, 23, 37, 39)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        order_id,
        unit_price,
        quantity_str
    FROM base
) src
UNPIVOT (
    metric_value FOR metric_name IN (unit_price, quantity_str)
);
