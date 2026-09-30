WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        unit_price,
        CORR(quantity, unit_price) OVER w_hist AS corr_quantity_unit_price,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price
    FROM order_items
    WINDOW w_hist AS (PARTITION BY product_id)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        unit_price,
        COUNT(unit_price) OVER (ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_unit_price,
        SUM(unit_price) OVER (ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_unit_price,
        AVG(unit_price) OVER (ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_unit_price,
        AVG(unit_price) OVER (ORDER BY first_known_unit_price ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS unit_price_avg_5
    FROM with_windows
    WHERE first_known_unit_price BETWEEN 180 AND 200
),
with_windows_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        unit_price,
        NTILE(5) OVER (ORDER BY running_sum_unit_price DESC, id ASC) AS running_sum_unit_price_bucket,
        FIRST_VALUE(running_count_unit_price, TRUE) OVER (ORDER BY running_count_unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_count_unit_price
    FROM scored
),
scored_order_items AS (
    -- Ranking and gap features
    SELECT
        id,
        unit_price,
        LEAD(first_known_running_count_unit_price) OVER (ORDER BY first_known_running_count_unit_price ASC, id ASC) AS next_first_known_running_count_unit_price,
        LEAD(running_sum_unit_price_bucket) OVER (ORDER BY first_known_running_count_unit_price ASC, id ASC) AS next_running_sum_unit_price_bucket,
        ROW_NUMBER() OVER (ORDER BY first_known_running_count_unit_price ASC, id ASC) AS seq_no
    FROM with_windows_order_items
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CEIL(next_first_known_running_count_unit_price) AS next_first_known_running_count_unit_price_whole
    FROM scored_order_items
    WHERE unit_price > 140
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER w_recent AS next_running_sum_unit_price_bucket_rank,
        AVG(next_running_sum_unit_price_bucket) OVER (ORDER BY next_first_known_running_count_unit_price ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS next_running_sum_unit_price_bucket_avg_3_prior
    FROM base
    WINDOW w_recent AS (ORDER BY next_running_sum_unit_price_bucket ASC NULLS FIRST)
)
-- Multi-level aggregation
SELECT
    seq_no,
    APPROX_PERCENTILE(next_running_sum_unit_price_bucket, 0.9) AS next_running_sum_unit_price_bucket_p90,
    MIN(next_first_known_running_count_unit_price) AS min_next_first_known_running_count_unit_price,
    COUNT(DISTINCT id) AS distinct_ids
FROM metrics
GROUP BY seq_no
HAVING min_next_first_known_running_count_unit_price < 1000000;
