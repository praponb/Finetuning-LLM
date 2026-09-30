WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        GREATEST(unit_price, quantity) AS greatest_unit_price_quantity,
        ROUND(unit_price * 1.1, 2) AS unit_price_adjusted
    FROM order_items
    WHERE product_id = 16
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        unit_price,
        unit_price_adjusted,
        LAG(unit_price, 1) OVER (ORDER BY greatest_unit_price_quantity, id) AS prev_unit_price,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY quantity DESC NULLS LAST) AS quantity_cume_dist
    FROM base
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY prev_unit_price, id) = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        unit_price,
        unit_price_adjusted,
        prev_unit_price,
        quantity_cume_dist,
        CASE WHEN prev_unit_price >= 420 THEN 'High' WHEN prev_unit_price >= 170 THEN 'Medium' ELSE 'Low' END AS prev_unit_price_band
    FROM ranked
    WHERE order_id IN (3, 17, 35)
),
cleaned_order_items AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        unit_price,
        quantity_cume_dist,
        unit_price - quantity_cume_dist AS unit_price_minus_quantity_cume_dist
    FROM cleaned
    WHERE prev_unit_price <= 390
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        quantity_cume_dist,
        unit_price_minus_quantity_cume_dist,
        LEAD(unit_price, 2, 0) OVER w_hist AS next_unit_price,
        ROW_NUMBER() OVER w_hist AS row_seq,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY unit_price_minus_quantity_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price
    FROM cleaned_order_items
    WHERE quantity_cume_dist <= 0.41
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY unit_price, id)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity_cume_dist,
        next_unit_price,
        row_seq,
        last_known_unit_price,
        LAG(row_seq) OVER (ORDER BY last_known_unit_price, id) AS prev_row_seq,
        LEAD(unit_price_minus_quantity_cume_dist) OVER (ORDER BY last_known_unit_price, id) AS next_unit_price_minus_quantity_cume_dist,
        LAG(unit_price_minus_quantity_cume_dist) OVER (ORDER BY last_known_unit_price, id) AS prev_unit_price_minus_quantity_cume_dist,
        ROW_NUMBER() OVER (ORDER BY last_known_unit_price, id) AS event_seq
    FROM scored
)
-- Grouped metrics
SELECT
    CASE WHEN prev_unit_price_minus_quantity_cume_dist >= 67 THEN 'LARGE' ELSE 'SMALL' END AS prev_unit_price_minus_quantity_cume_dist_tier,
    CASE WHEN next_unit_price >= 390 THEN 'LARGE' ELSE 'SMALL' END AS next_unit_price_tier,
    ROUND(AVG(next_unit_price_minus_quantity_cume_dist), 2) AS avg_next_unit_price_minus_quantity_cume_dist,
    ROUND(AVG(last_known_unit_price), 2) AS avg_last_known_unit_price,
    SUM(last_known_unit_price) AS total_last_known_unit_price
FROM enriched
GROUP BY CASE WHEN prev_unit_price_minus_quantity_cume_dist >= 67 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN next_unit_price >= 390 THEN 'LARGE' ELSE 'SMALL' END
