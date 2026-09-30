WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        method,
        amount,
        LEAD(amount, 1) OVER (PARTITION BY TO_DATE(paid_at), status ORDER BY paid_at ASC, id ASC) AS next_amount,
        LEAD(amount, 1) OVER (PARTITION BY TO_DATE(paid_at), method ORDER BY paid_at ASC, id ASC) AS next_amount_2,
        NTILE(5) OVER (PARTITION BY status ORDER BY amount ASC, id ASC) AS amount_bucket,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount ASC, id ASC) AS event_seq
    FROM payments
    WHERE order_id IN (12, 24, 37)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        method,
        amount,
        next_amount,
        LAG(next_amount, 1, 0) OVER (PARTITION BY method ORDER BY event_seq ASC, id ASC) AS prev_next_amount,
        NTILE(5) OVER (PARTITION BY method ORDER BY next_amount DESC, id ASC) AS next_amount_bucket,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY method ORDER BY next_amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
    FROM enriched
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY next_amount DESC NULLS LAST, id ASC) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
),
latest_payments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY method ORDER BY latest_rank ASC) AS rank_in_group
        FROM latest
    ) ranked
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(next_amount_bucket) OVER (PARTITION BY method ORDER BY amount ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_next_amount_bucket,
        MIN(next_amount_bucket) OVER (PARTITION BY method ORDER BY amount ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_next_amount_bucket,
        COUNT(next_amount_bucket) OVER (PARTITION BY method ORDER BY amount ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_next_amount_bucket,
        SUM(next_amount_bucket) OVER (PARTITION BY method ORDER BY amount ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_next_amount_bucket,
        SUM(prev_next_amount) OVER (PARTITION BY method ORDER BY next_amount_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_next_amount
    FROM latest_payments
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY last3_count_next_amount_bucket DESC NULLS LAST, id ASC) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        amount,
        prev_next_amount,
        latest_rank,
        rank_in_group,
        last3_max_next_amount_bucket,
        last3_min_next_amount_bucket,
        last3_count_next_amount_bucket,
        last3_sum_next_amount_bucket,
        ROUND(last3_count_next_amount_bucket, 2) AS last3_count_next_amount_bucket_rounded
    FROM deduped
    WHERE next_amount_bucket < 4
)
-- Grouped metrics
SELECT
    method,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list,
    COUNT_IF(method = 'cash') AS cash_events,
    APPROX_PERCENTILE(prev_next_amount, ARRAY(0.25, 0.5, 0.75)) AS prev_next_amount_quartiles
FROM cleaned
GROUP BY 1
ORDER BY cash_events ASC NULLS LAST
