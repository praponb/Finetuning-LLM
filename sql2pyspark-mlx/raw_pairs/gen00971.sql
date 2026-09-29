WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(unit_price * 0.85, 2) AS unit_price_adjusted
    FROM order_items
    WHERE product_id IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(unit_price * 1.07, 2) AS adjusted_unit_price
    FROM valid
    WHERE product_id IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price_adjusted,
        adjusted_unit_price,
        LAG(quantity) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS prev_quantity,
        LEAD(unit_price_adjusted) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS next_unit_price_adjusted,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) AS seq_no
    FROM cleaned
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        quantity,
        unit_price_adjusted,
        adjusted_unit_price,
        AVG(adjusted_unit_price) OVER w_hist AS adjusted_unit_price_avg_3,
        ROW_NUMBER() OVER w_running AS seq_no_2
    FROM enriched
    WINDOW w_hist AS (ORDER BY prev_quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY order_id, product_id ORDER BY adjusted_unit_price, id)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        quantity,
        unit_price_adjusted,
        adjusted_unit_price,
        adjusted_unit_price_avg_3,
        seq_no_2,
        ROUND(unit_price_adjusted * 100, 2) AS unit_price_adjusted_adjusted
    FROM scored
    WHERE order_id = 22
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        adjusted_unit_price_avg_3,
        seq_no_2,
        unit_price_adjusted_adjusted,
        SUM(adjusted_unit_price) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_adjusted_unit_price,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY adjusted_unit_price DESC, id) AS event_seq,
        SUM(quantity) OVER (ORDER BY unit_price_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        LEAD(unit_price_adjusted_adjusted, 1) OVER w_running AS next_unit_price_adjusted_adjusted
    FROM filtered
    WINDOW w_running AS (PARTITION BY order_id ORDER BY adjusted_unit_price_avg_3, id)
),
scored_order_items AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(adjusted_unit_price_avg_3) OVER w_part AS next_adjusted_unit_price_avg_3,
        LAG(adjusted_unit_price_avg_3) OVER w_part AS prev_adjusted_unit_price_avg_3,
        LEAD(cumulative_quantity) OVER w_part AS next_cumulative_quantity,
        ROW_NUMBER() OVER w_part AS row_seq
    FROM with_windows
    WINDOW w_part AS (ORDER BY unit_price_adjusted_adjusted, id)
)
SELECT
    id,
    adjusted_unit_price_avg_3,
    seq_no_2,
    unit_price_adjusted_adjusted,
    cumulative_adjusted_unit_price,
    event_seq,
    cumulative_quantity,
    next_unit_price_adjusted_adjusted,
    next_adjusted_unit_price_avg_3
FROM scored_order_items
ORDER BY cumulative_adjusted_unit_price DESC NULLS LAST;
