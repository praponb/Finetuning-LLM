WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(quantity, 2) AS quantity_rounded
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        quantity,
        quantity_rounded,
        CORR(quantity, unit_price) OVER (PARTITION BY order_id, product_id) AS corr_quantity_unit_price,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity_rounded DESC, id) AS seq_no,
        NTILE(4) OVER (ORDER BY quantity, id) AS quantity_bucket,
        ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY quantity, id) AS event_seq
    FROM cleaned
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        quantity_rounded,
        event_seq,
        AVG(corr_quantity_unit_price) OVER (PARTITION BY order_id) AS order_id_avg_corr_quantity_unit_price,
        AVG(seq_no) OVER (PARTITION BY order_id ORDER BY seq_no, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS seq_no_avg_3_prior
    FROM with_windows
    WHERE corr_quantity_unit_price > 0.5
),
cleaned_order_items AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(seq_no_avg_3_prior, 0) AS seq_no_avg_3_prior_rounded
    FROM scored
    WHERE event_seq < 7
)
-- Multi-level aggregation
SELECT
    CASE WHEN quantity_rounded >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_rounded_tier,
    CASE WHEN order_id_avg_corr_quantity_unit_price >= 1 THEN 'LARGE' ELSE 'SMALL' END AS order_id_avg_corr_quantity_unit_price_tier,
    ROUND(AVG(event_seq), 2) AS avg_event_seq,
    PERCENTILE_APPROX(seq_no_avg_3_prior, ARRAY(0.25, 0.5, 0.75)) AS seq_no_avg_3_prior_quartiles,
    ROUND(AVG(order_id_avg_corr_quantity_unit_price), 2) AS avg_order_id_avg_corr_quantity_unit_price,
    PERCENTILE_APPROX(seq_no_avg_3_prior, 0.95) AS seq_no_avg_3_prior_p95,
    APPROX_PERCENTILE(quantity_rounded, 0.95) AS quantity_rounded_p95
FROM cleaned_order_items
GROUP BY ROLLUP (CASE WHEN quantity_rounded >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN order_id_avg_corr_quantity_unit_price >= 1 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY avg_order_id_avg_corr_quantity_unit_price DESC NULLS LAST;
