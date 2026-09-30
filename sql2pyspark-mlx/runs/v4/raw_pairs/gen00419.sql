WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, quantity - unit_price AS line_total
    FROM order_items
    WHERE order_id IN (19, 21, 37)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (ORDER BY unit_price, id) AS unit_price_bucket,
        ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY quantity, id) AS seq_no
    FROM cleaned
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_hist AS seq_no_rank,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY unit_price_bucket NULLS FIRST) AS unit_price_bucket_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price_bucket DESC, id) AS seq_no_2,
        SUM(unit_price_bucket) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price_bucket,
        AVG(unit_price_bucket) OVER w_ordered AS unit_price_bucket_avg_6_prior
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY seq_no DESC), w_ordered AS (PARTITION BY product_id ORDER BY line_total, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id) AS rn
    FROM enriched
    QUALIFY rn = 1
)
-- Ranking and gap features
SELECT
    *,
    NTILE(4) OVER (ORDER BY running_unit_price_bucket, id) AS running_unit_price_bucket_bucket,
    SUM(seq_no_2) OVER (ORDER BY seq_no_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no_2,
    CORR(quantity, unit_price) OVER (PARTITION BY order_id) AS corr_quantity_unit_price
FROM deduped;
