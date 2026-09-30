WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(quantity) AS quantity_whole
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        quantity_whole,
        CUME_DIST() OVER (ORDER BY quantity) AS quantity_cume_dist,
        LAG(quantity_whole, 1) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS prev_quantity_whole,
        CORR(unit_price, quantity) OVER (PARTITION BY order_id, product_id) AS corr_unit_price_quantity,
        NTILE(3) OVER (PARTITION BY order_id, product_id ORDER BY quantity DESC, id) AS quantity_bucket
    FROM filtered
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(prev_quantity_whole) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS next_prev_quantity_whole,
        LEAD(quantity_bucket) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS next_quantity_bucket,
        LAG(quantity_bucket) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS prev_quantity_bucket,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY corr_unit_price_quantity DESC, id) AS event_seq_2
    FROM with_windows
    WHERE corr_unit_price_quantity >= -0.3
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        unit_price,
        quantity_whole,
        quantity_cume_dist,
        prev_quantity_whole,
        corr_unit_price_quantity,
        quantity_bucket,
        prev_quantity_bucket,
        event_seq,
        event_seq_2,
        GREATEST(event_seq_2, prev_quantity_whole) AS greatest_event_seq_2_prev_quantity_whole
    FROM enriched
    WHERE event_seq_2 BETWEEN 7 AND 8
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        unit_price,
        quantity_whole,
        quantity_cume_dist,
        prev_quantity_bucket,
        event_seq_2,
        greatest_event_seq_2_prev_quantity_whole,
        CASE WHEN unit_price >= 430 THEN 'High' WHEN unit_price >= 170 THEN 'Medium' ELSE 'Low' END AS unit_price_band
    FROM cleaned
    WHERE product_id IS NOT NULL
),
base_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(unit_price AS INT) AS unit_price_int
    FROM base
    WHERE unit_price BETWEEN 220 AND 320
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        unit_price,
        greatest_event_seq_2_prev_quantity_whole,
        quantity_cume_dist
    FROM base_order_items
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (unit_price AS UNIT_PRICE, greatest_event_seq_2_prev_quantity_whole AS GREATEST_EVENT_SEQ_2_PREV_QUANTITY_WHOLE_VAL, quantity_cume_dist)
)
