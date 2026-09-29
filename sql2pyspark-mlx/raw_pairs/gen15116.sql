WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        segment,
        signup_date,
        ROUND(age / SUM(age) OVER (PARTITION BY city, segment), 4) AS age_share,
        LAG(age, 1) OVER (PARTITION BY segment ORDER BY signup_date, id) AS prev_age,
        NTILE(10) OVER (PARTITION BY segment ORDER BY age, id) AS age_bucket,
        LAST_VALUE(credit_limit, TRUE) OVER (PARTITION BY YEAR(signup_date) ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit
    FROM customers
    WHERE age IS NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(age_bucket) OVER (PARTITION BY segment ORDER BY age_share, id) AS prev_age_bucket,
        LAG(last_known_credit_limit) OVER (PARTITION BY segment ORDER BY age_share, id) AS prev_last_known_credit_limit,
        LEAD(last_known_credit_limit) OVER (PARTITION BY segment ORDER BY age_share, id) AS next_last_known_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age_share, id) AS seq_no
    FROM scored
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        prev_age * seq_no AS prev_age_times_seq_no,
        CAST(age_bucket AS STRING) AS age_bucket_num,
        COALESCE(segment, 'N/A') AS segment_clean
    FROM enriched
    WHERE signup_date > DATE '2023-03-05'
      AND prev_age <> 61
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.order_date < s.signup_date
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(prev_last_known_credit_limit) OVER w_grp AS running_prev_last_known_credit_limit,
        MAX(prev_age_bucket) OVER w_recent AS segment_max_prev_age_bucket
    FROM not_flagged
    WINDOW w_grp AS (PARTITION BY segment ORDER BY age_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY segment)
)
-- Grouped metrics
SELECT
    segment,
    MAX(prev_age_times_seq_no) FILTER (WHERE segment = 'SMB') AS smb_max_prev_age_times_seq_no,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(running_prev_last_known_credit_limit, 0.95) AS running_prev_last_known_credit_limit_p95
FROM metrics
GROUP BY segment
ORDER BY distinct_ids DESC NULLS LAST;
