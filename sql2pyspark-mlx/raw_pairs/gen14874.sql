WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        signup_date,
        credit_limit,
        INITCAP(TRIM(city)) city_title
    FROM customers
    WHERE signup_date >= DATE '2023-03-15'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.amount >= 1420
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(email) clean_email,
        DATE_FORMAT(signup_date, 'MMM yyyy') signup_date_label
    FROM not_flagged
    WHERE signup_date > DATE '2023-05-24'
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.credit_limit,
        s.city_title,
        s.clean_email,
        s.signup_date_label,
        top1.plan
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.plan
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(credit_limit * 1.07, 2) credit_limit_scaled
    FROM with_last
    WHERE plan IS DISTINCT FROM 'basic'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        signup_date_label,
        plan,
        CUME_DIST() OVER (ORDER BY credit_limit DESC NULLS LAST) credit_limit_cume_dist,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY credit_limit_scaled) credit_limit_scaled_dense_rank,
        AVG(credit_limit) OVER (PARTITION BY plan ORDER BY credit_limit_scaled, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) credit_limit_avg_3_prior,
        NTILE(4) OVER (PARTITION BY plan ORDER BY credit_limit_scaled DESC, id) credit_limit_scaled_bucket,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY credit_limit DESC NULLS FIRST) credit_limit_percent_rank
    FROM base
    WHERE credit_limit_scaled IS NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY credit_limit_cume_dist DESC NULLS FIRST, id) AS row_num
        FROM with_windows
    ) x
    WHERE row_num = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN credit_limit_scaled_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_scaled_dense_rank_tier,
    CASE WHEN credit_limit_avg_3_prior >= 12449 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_avg_3_prior_tier,
    plan,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    ROUND(AVG(credit_limit_percent_rank), 2) AS avg_credit_limit_percent_rank,
    ROUND(AVG(credit_limit_cume_dist), 2) AS avg_credit_limit_cume_dist,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN credit_limit_scaled_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END ORDER BY ROUND(AVG(credit_limit_percent_rank), 2) DESC) AS avg_credit_limit_percent_rank_dense_rank
FROM ranked
GROUP BY CASE WHEN credit_limit_scaled_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN credit_limit_avg_3_prior >= 12449 THEN 'PREMIUM' ELSE 'STANDARD' END, plan
ORDER BY credit_limit_avg_3_prior_tier NULLS LAST
