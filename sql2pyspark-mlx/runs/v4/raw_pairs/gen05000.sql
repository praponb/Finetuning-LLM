WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        quantity * unit_price AS line_total,
        quantity - unit_price AS line_total_2
    FROM order_items
    WHERE product_id IS NOT NULL
),
cleaned_order_items AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(quantity, 2) AS quantity_rounded
    FROM cleaned
    WHERE line_total > 62
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        quantity + line_total_2 AS quantity_plus_line_total_2,
        FLOOR(line_total) AS line_total_whole
    FROM cleaned_order_items
    WHERE product_id IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(line_total_whole) IGNORE NULLS OVER (PARTITION BY order_id, product_id ORDER BY quantity_plus_line_total_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_line_total_whole,
        AVG(quantity_plus_line_total_2) OVER (PARTITION BY product_id ORDER BY line_total, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_plus_line_total_2_avg_6_prior
    FROM base
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        line_total,
        quantity_rounded,
        line_total_whole,
        last_known_line_total_whole,
        quantity_plus_line_total_2_avg_6_prior,
        ROUND(line_total_2 * 1.15, 2) AS line_total_2_adjusted
    FROM with_windows
    WHERE product_id IS NOT NULL
),
stats AS (
    -- Grouped metrics
    SELECT
        product_id,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(quantity_rounded, 0.95) AS quantity_rounded_p95,
        AVG(last_known_line_total_whole) AS avg_last_known_line_total_whole,
        PERCENTILE_APPROX(line_total_whole, 0.9) AS line_total_whole_p90
    FROM filtered
    GROUP BY product_id
)
SELECT
    s.product_id,
    s.distinct_ids,
    s.quantity_rounded_p95,
    s.avg_last_known_line_total_whole,
    s.line_total_whole_p90,
    (SELECT MAX(x.quantity) FROM inventory x WHERE x.product_id = s.product_id) AS max_inventory_quantity
FROM stats s
