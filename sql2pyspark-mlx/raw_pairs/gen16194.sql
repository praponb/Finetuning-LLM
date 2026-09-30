WITH base AS (
    SELECT
        *,
        UPPER(TRIM(name)) AS name_normalized,
        CASE WHEN credit_limit >= 13200 THEN 'High' WHEN credit_limit >= 5200 THEN 'Medium' ELSE 'Low' END AS credit_limit_level,
        COALESCE(segment, 'OTHER') AS resolved_segment
    FROM customers
    WHERE segment IS NOT NULL
),
with_last AS (
    SELECT
        s.*,
        top1.start_date,
        top1.subscription_plan
    FROM base s
    JOIN LATERAL (
            SELECT c.start_date, c.plan AS subscription_plan
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    SELECT
        id,
        name,
        city,
        signup_date,
        is_active,
        age,
        name_normalized,
        credit_limit_level,
        resolved_segment,
        start_date,
        DENSE_RANK() OVER (ORDER BY credit_limit DESC NULLS FIRST) AS credit_limit_dense_rank,
        NTILE(3) OVER (PARTITION BY segment, city ORDER BY credit_limit DESC, id ASC) AS credit_limit_bucket
    FROM with_last
),
filtered AS (
    SELECT
        id,
        name,
        is_active,
        age,
        name_normalized,
        credit_limit_level,
        resolved_segment,
        start_date,
        credit_limit_dense_rank,
        credit_limit_bucket,
        UPPER(city) AS city_upper,
        DATEDIFF(start_date, signup_date) / 7 AS weeks_signup_date_to_start_date
    FROM with_windows
    WHERE credit_limit_bucket < 6
)
SELECT
    DATE_TRUNC('YEAR', start_date) AS year_start,
    CASE WHEN credit_limit_bucket >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_bucket_tier,
    CASE WHEN age >= 65 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
    PERCENTILE_APPROX(credit_limit_dense_rank, 0.5) AS credit_limit_dense_rank_p50,
    APPROX_PERCENTILE(credit_limit_bucket, 0.9) AS credit_limit_bucket_p90,
    COUNT(*) AS event_count
FROM filtered
GROUP BY DATE_TRUNC('YEAR', start_date), CASE WHEN credit_limit_bucket >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN age >= 65 THEN 'HIGH' ELSE 'LOW' END
ORDER BY credit_limit_dense_rank_p50 ASC
