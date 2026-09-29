WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(name) name_upper,
        CONCAT(email, ' - ', name) email_name
    FROM customers
    WHERE city IS DISTINCT FROM 'Bangkok'
      AND credit_limit <= 14200
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.discount IS NULL
        )
),
latest_match AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.email,
        s.city,
        s.segment,
        s.signup_date,
        s.age,
        s.name_upper,
        top1.balance
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.balance
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
cleaned_customers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(TRIM(email)) email_normalized,
        age / NULLIF(balance, 0) age_per_balance,
        COALESCE(segment, 'NONE') segment_filled
    FROM latest_match
    WHERE signup_date >= DATE '2023-03-13'
      AND city IS DISTINCT FROM 'Tokyo'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(age / SUM(age) OVER (PARTITION BY DATE_TRUNC('WEEK', signup_date), segment), 4) age_share,
        NTILE(4) OVER (PARTITION BY city ORDER BY age_per_balance, id) age_per_balance_bucket,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY age_per_balance) age_per_balance_dense_rank
    FROM cleaned_customers
    WHERE balance >= 28000
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age DESC NULLS FIRST, id) AS latest_rank
        FROM with_windows
    ) t
    WHERE latest_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date, id) event_seq,
        LAST_VALUE(segment) IGNORE NULLS OVER (ORDER BY age_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_segment,
        SUM(age) OVER (PARTITION BY city ORDER BY age_per_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_age,
        NTILE(4) OVER (PARTITION BY city ORDER BY age_per_balance, id) age_per_balance_bucket_2,
        LAG(age_per_balance, 1) OVER (PARTITION BY segment ORDER BY balance, id) prev_age_per_balance
    FROM ranked
)
-- Multi-level aggregation
SELECT
    CASE WHEN age_per_balance >= 65 THEN 'HIGH' ELSE 'LOW' END AS age_per_balance_tier,
    COUNT(*) FILTER (WHERE segment = 'Corporate') AS corporate_rows,
    APPROX_PERCENTILE(age_per_balance_bucket_2, 0.9) AS age_per_balance_bucket_2_p90,
    ROUND(AVG(age), 2) AS avg_age
FROM enriched
GROUP BY CASE WHEN age_per_balance >= 65 THEN 'HIGH' ELSE 'LOW' END
HAVING APPROX_PERCENTILE(age_per_balance_bucket_2, 0.9) < 1000000
ORDER BY age_per_balance_tier DESC
