WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(unit_price / quantity, 2) AS line_total,
        COALESCE(quantity, 0) AS quantity_filled
    FROM order_items
    WHERE order_id IN (4, 27)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        quantity,
        quantity_filled
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(quantity, 1) OVER w_recent AS prev_quantity,
        SUM(quantity) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        MAX(quantity) OVER (PARTITION BY order_id) AS order_id_max_quantity,
        SUM(quantity_filled) OVER (PARTITION BY order_id ORDER BY quantity_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_filled
    FROM ranked
    WHERE quantity = 7
    WINDOW w_recent AS (ORDER BY quantity, id)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        quantity,
        order_id_max_quantity,
        cumulative_quantity_filled
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cumulative_quantity DESC, id) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(cumulative_quantity_filled) OVER (ORDER BY order_id_max_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_quantity_filled,
        LAG(cumulative_quantity_filled, 2) OVER (ORDER BY order_id_max_quantity, id) AS prev_cumulative_quantity_filled
    FROM latest
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows
    EXCEPT DISTINCT
    SELECT id
    FROM order_items
    WHERE unit_price BETWEEN 290 AND 360
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN cumulative_quantity_filled >= 368 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_quantity_filled_tier,
        PERCENTILE_APPROX(cumulative_quantity_filled, ARRAY(0.25, 0.5, 0.75)) AS cumulative_quantity_filled_quartiles,
        BIT_AND(quantity) AS quantity_bit_and,
        ROUND(AVG(order_id_max_quantity), 2) AS avg_order_id_max_quantity
    FROM reconciled
    GROUP BY cumulative_quantity_filled_tier
)
SELECT
    cumulative_quantity_filled_tier,
    cumulative_quantity_filled_quartiles,
    quantity_bit_and
FROM stats
ORDER BY cumulative_quantity_filled_tier
