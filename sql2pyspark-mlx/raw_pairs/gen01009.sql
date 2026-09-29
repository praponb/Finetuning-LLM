WITH valid AS (
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        COALESCE(quantity, 0) AS quantity_filled,
        ROUND(quantity / unit_price, 2) AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
)
SELECT *
FROM (
    SELECT
        id,
        product_id,
        unit_price,
        quantity_filled
    FROM valid
) src
UNPIVOT (
    reading FOR metric_name IN (unit_price, quantity_filled)
) unp
