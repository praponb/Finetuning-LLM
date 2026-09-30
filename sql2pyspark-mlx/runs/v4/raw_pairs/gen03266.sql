WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM subscriptions
    WHERE start_date >= DATE '2024-02-24'
      AND start_date BETWEEN '2023-08-01' AND '2025-01-26'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date, id) AS row_seq,
        LAST_VALUE(days_start_date_to_end_date) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_start_date_to_end_date
    FROM cleaned
),
enriched AS (
    -- Attach customers attributes
    SELECT src.*, c.segment
    FROM metrics src
    INNER JOIN customers c
        ON src.customer_id = c.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        end_date,
        monthly_fee,
        days_start_date_to_end_date,
        row_seq,
        last_known_days_start_date_to_end_date,
        segment,
        last_known_days_start_date_to_end_date - days_start_date_to_end_date AS last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date,
        COALESCE(segment, 'N/A') AS segment_clean
    FROM enriched
    WHERE customer_id IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(monthly_fee, 1) AS monthly_fee_rounded
    FROM filtered
    WHERE end_date > DATE '2023-06-06'
      AND segment IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date, 2, 0) OVER (PARTITION BY segment ORDER BY row_seq, id) AS next_last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date,
        FIRST_VALUE(last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date, TRUE) OVER w_grp AS first_known_last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date,
        FIRST_VALUE(monthly_fee_rounded) IGNORE NULLS OVER (PARTITION BY segment ORDER BY monthly_fee_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee_rounded
    FROM base
    WINDOW w_grp AS (PARTITION BY segment ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(first_known_monthly_fee_rounded) OVER (PARTITION BY segment) AS segment_sum_first_known_monthly_fee_rounded,
        LAST_VALUE(first_known_last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date) IGNORE NULLS OVER (PARTITION BY segment ORDER BY last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_known_last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date
    FROM with_windows
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        segment,
        DATE_TRUNC('WEEK', end_date) AS week_start,
        AVG(first_known_last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date) FILTER (WHERE segment = 'Consumer') AS consumer_avg_first_known_last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date,
        APPROX_PERCENTILE(row_seq, 0.95) AS row_seq_p95,
        COUNT(*) FILTER (WHERE segment = 'Corporate') AS corporate_rows,
        COUNT_IF(segment = 'SMB') AS smb_events,
        RANK() OVER (PARTITION BY segment ORDER BY APPROX_PERCENTILE(row_seq, 0.95) DESC) AS row_seq_p95_rank
    FROM enriched_subscriptions
    GROUP BY segment, DATE_TRUNC('WEEK', end_date)
)
SELECT
    segment,
    week_start,
    row_seq_p95,
    corporate_rows,
    row_seq_p95_rank,
    CASE WHEN segment RLIKE '^(SMB|Consumer)$' THEN 'GROUP_B' WHEN segment RLIKE '^(Corporate)$' THEN 'EXTENDED' ELSE 'CORE' END AS segment_zone
FROM summary;
