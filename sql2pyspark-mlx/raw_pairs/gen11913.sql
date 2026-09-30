WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        method,
        amount,
        paid_at,
        SUM(amount) OVER (PARTITION BY order_id, method ORDER BY CAST(paid_at AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_sum_90d,
        AVG(amount) OVER (PARTITION BY order_id, method ORDER BY paid_at ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_avg_6_prior,
        AVG(amount) OVER (ORDER BY paid_at ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior
    FROM payments
    WHERE order_id IN (6, 7, 25)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(method) AS method_length,
        FLOOR(amount_avg_2_prior) AS amount_avg_2_prior_whole,
        COALESCE(method, 'UNKNOWN') AS method_filled
    FROM enriched
    WHERE paid_at >= DATE '2023-05-21'
      AND method IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        amount,
        amount_sum_90d,
        amount_avg_6_prior,
        amount_avg_2_prior,
        method_length,
        amount_avg_2_prior_whole,
        method_filled,
        DATE_TRUNC('WEEK', paid_at) AS paid_at_period
    FROM filtered
    WHERE paid_at > TO_DATE('2023-06-15')
      AND method IS DISTINCT FROM 'promptpay'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        amount,
        amount_sum_90d,
        amount_avg_6_prior,
        amount_avg_2_prior,
        paid_at_period,
        CAST(amount_avg_2_prior_whole AS STRING) AS amount_avg_2_prior_whole_str,
        COALESCE(method, 'OTHER') AS resolved_method
    FROM valid
    WHERE method IS NOT DISTINCT FROM 'card'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(amount_avg_2_prior) OVER w_recent AS running_max_amount_avg_2_prior,
        AVG(amount_avg_2_prior) OVER w_recent AS running_avg_amount_avg_2_prior,
        COUNT(amount_avg_2_prior) OVER w_recent AS running_count_amount_avg_2_prior
    FROM cleaned
    WINDOW w_recent AS (ORDER BY amount_sum_90d ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows
    EXCEPT DISTINCT
    SELECT id
    FROM payments
    WHERE method = 'card'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN matched_keys k ON s.id = k.id
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('MONTH', paid_at_period) AS activity_month,
        PERCENTILE_APPROX(amount_avg_6_prior, 0.5) AS amount_avg_6_prior_p50,
        (UNIX_TIMESTAMP(MAX(paid_at_period)) - UNIX_TIMESTAMP(MIN(paid_at_period))) / 60 AS span_minutes,
        COUNT(*) AS event_count,
        APPROX_PERCENTILE(amount_sum_90d, 0.9) AS amount_sum_90d_p90
    FROM kept_rows
    GROUP BY 1
)
SELECT
    activity_month,
    span_minutes,
    amount_sum_90d_p90
FROM summary
ORDER BY amount_sum_90d_p90 DESC NULLS FIRST
