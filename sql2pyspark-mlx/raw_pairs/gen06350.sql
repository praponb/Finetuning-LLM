WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        ROUND(unit_price / quantity, 2) AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
)
-- Running and moving aggregates
SELECT
    id,
    order_id,
    product_id,
    unit_price,
    line_total,
    LAG(unit_price, 1, 0) OVER (PARTITION BY order_id ORDER BY line_total, id) AS prev_unit_price,
    AVG(line_total) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS line_total_avg_3_prior
FROM filtered
