WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        ADD_MONTHS(end_date, 6) AS end_date_plus_months,
        DATE_SUB(start_date, 86) AS start_date_minus_86d
    FROM subscriptions
    WHERE start_date > DATE '2023-08-19'
      AND customer_id IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        status,
        end_date_plus_months,
        CASE WHEN monthly_fee >= 210 THEN 'High' WHEN monthly_fee >= 57 THEN 'Medium' ELSE 'Low' END AS monthly_fee_tier,
        UPPER(status) AS status_upper
    FROM valid
    WHERE MONTH(end_date) IN (3, 6, 8)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY monthly_fee DESC) AS monthly_fee_percent_rank,
        NTILE(3) OVER w_grp AS monthly_fee_bucket,
        NTILE(5) OVER w_grp AS monthly_fee_bucket_2,
        ROW_NUMBER() OVER w_grp AS row_seq,
        FIRST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee
    FROM cleaned
    WHERE MONTH(start_date) IN (5, 10, 12)
    WINDOW w_grp AS (PARTITION BY status ORDER BY monthly_fee DESC, id)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        status,
        status_upper,
        monthly_fee_bucket,
        LEAD(monthly_fee_bucket_2) OVER w_part AS next_monthly_fee_bucket_2,
        LAG(monthly_fee_bucket_2) OVER w_part AS prev_monthly_fee_bucket_2,
        LEAD(first_known_monthly_fee) OVER w_part AS next_first_known_monthly_fee,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM enriched
    WINDOW w_part AS (PARTITION BY status ORDER BY monthly_fee, id)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        status,
        status_upper,
        monthly_fee_bucket,
        next_monthly_fee_bucket_2,
        next_first_known_monthly_fee,
        seq_no,
        COALESCE(seq_no, 0) AS seq_no_filled
    FROM scored
    WHERE seq_no BETWEEN 5 AND 6
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY monthly_fee DESC NULLS LAST) = 1
),
enriched_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        start_date,
        monthly_fee,
        status_upper,
        monthly_fee_bucket,
        next_monthly_fee_bucket_2,
        next_first_known_monthly_fee,
        LAG(seq_no_filled) OVER (PARTITION BY status ORDER BY seq_no, id) AS prev_seq_no_filled,
        LEAD(monthly_fee_bucket) OVER (PARTITION BY status ORDER BY seq_no, id) AS next_monthly_fee_bucket,
        LAG(monthly_fee_bucket) OVER (PARTITION BY status ORDER BY seq_no, id) AS prev_monthly_fee_bucket,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY seq_no, id) AS seq_no_2,
        LEAD(seq_no_filled, 1) OVER (PARTITION BY status ORDER BY next_first_known_monthly_fee, id) AS next_seq_no_filled
    FROM latest
)
-- Grouped metrics
SELECT
    CASE WHEN monthly_fee_bucket >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_bucket_tier,
    DATE_TRUNC('MONTH', start_date) AS activity_month,
    ROUND(AVG(prev_monthly_fee_bucket), 2) AS avg_prev_monthly_fee_bucket,
    PERCENTILE_APPROX(next_monthly_fee_bucket_2, 0.5) AS next_monthly_fee_bucket_2_p50,
    PERCENTILE_APPROX(next_monthly_fee_bucket, 0.5) AS next_monthly_fee_bucket_p50
FROM enriched_subscriptions
GROUP BY monthly_fee_bucket_tier, activity_month
ORDER BY monthly_fee_bucket_tier DESC;
