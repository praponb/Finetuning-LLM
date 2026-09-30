WITH filtered AS (
    SELECT
        *,
        quantity * unit_price AS line_total,
        unit_price - quantity AS line_total_2
    FROM order_items
    WHERE product_id IS NULL
)
SELECT
    d.category,
    COUNT(*) AS rows_total,
    MIN(src.quantity) AS min_quantity,
    MAX(src.line_total_2) AS max_line_total_2
FROM filtered src
JOIN products d ON src.product_id = d.id
GROUP BY d.category;
