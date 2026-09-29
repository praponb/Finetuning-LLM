WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        amount,
        paid_at,
        DATE_FORMAT(paid_at, 'yyyy-MM-dd') paid_at_label
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-10-09 17:00:00'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        paid_at,
        COALESCE(amount, 0) amount_filled
    FROM filtered
    WHERE paid_at >= DATE '2023-09-29'
      AND method IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        method,
        paid_at,
        FIRST_VALUE(method, TRUE) OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_method,
        LEAD(amount_filled, 1) OVER w_recent next_amount_filled,
        LAST_VALUE(method, TRUE) OVER (PARTITION BY MONTH(paid_at), method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_method
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY method ORDER BY amount_filled, id)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM metrics
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY method ORDER BY is_new_burst NULLS FIRST) is_new_burst_percent_rank,
        SUM(is_new_burst) OVER (PARTITION BY DATE_TRUNC('YEAR', paid_at), method) paid_at_period_sum_is_new_burst,
        RANK() OVER (PARTITION BY method ORDER BY next_amount_filled) next_amount_filled_rank
    FROM sessionized
    WHERE method NOT IN ('bank_transfer', 'cash')
)
-- Ranking and gap features
SELECT
    id,
    method,
    paid_at,
    first_known_method,
    last_known_method,
    is_new_burst,
    burst_id,
    is_new_burst_percent_rank,
    next_amount_filled_rank,
    AVG(next_amount_filled) OVER (PARTITION BY method ORDER BY is_new_burst, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) next_amount_filled_avg_6_prior,
    AVG(is_new_burst_percent_rank) OVER (PARTITION BY method ORDER BY is_new_burst, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) is_new_burst_percent_rank_avg_3,
    AVG(is_new_burst_percent_rank) OVER (PARTITION BY method ORDER BY is_new_burst_percent_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) is_new_burst_percent_rank_avg_2,
    NTILE(5) OVER (PARTITION BY method ORDER BY is_new_burst, id) is_new_burst_bucket,
    LEAD(next_amount_filled_rank, 1) OVER (PARTITION BY method ORDER BY is_new_burst_percent_rank, id) next_next_amount_filled_rank
FROM with_windows;
