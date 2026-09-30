WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(unit_price / quantity, 2) AS line_total
    FROM order_items
    WHERE order_id IN (21, 29)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(unit_price * 1.07, 2) AS unit_price_scaled
    FROM filtered
    WHERE product_id IS NOT NULL
),
cleaned_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        line_total,
        unit_price_scaled,
        CEIL(unit_price) AS unit_price_whole,
        CAST(quantity AS STRING) AS quantity_str
    FROM cleaned
    WHERE order_id IN (5, 40)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY line_total DESC, id) AS latest_rank
    FROM cleaned_order_items
    QUALIFY latest_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        line_total,
        unit_price_scaled,
        unit_price_whole,
        quantity_str,
        latest_rank,
        DENSE_RANK() OVER w_running AS unit_price_scaled_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY line_total DESC, id) AS event_seq
    FROM deduped
    WINDOW w_running AS (PARTITION BY product_id ORDER BY unit_price_scaled NULLS FIRST)
)
-- Grouped metrics
SELECT
    order_id,
    ROUND(AVG(unit_price_scaled), 2) AS avg_unit_price_scaled,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows
FROM metrics
GROUP BY order_id
