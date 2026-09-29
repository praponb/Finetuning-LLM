WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(status) AS status_length,
        CAST(monthly_fee AS INT) AS monthly_fee_int
    FROM subscriptions
    WHERE start_date >= TO_DATE('2023-08-06')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        end_date,
        status_length
    FROM base
    QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY end_date DESC) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        status_length,
        NTILE(3) OVER (ORDER BY status_length, id) AS status_length_bucket,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY status_length DESC, id) AS status_length_bucket_2
    FROM latest
),
base_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(status_length_bucket_2, 0) AS status_length_bucket_2_filled
    FROM metrics
    WHERE status_length_bucket BETWEEN 6 AND 8
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN status_length_bucket_2_filled >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_length_bucket_2_filled_tier,
        CASE WHEN status_length >= 87 THEN 'HIGH' ELSE 'LOW' END AS status_length_tier,
        CASE WHEN status_length_bucket_2 >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_length_bucket_2_tier,
        PERCENTILE_APPROX(status_length_bucket, 0.9) AS status_length_bucket_p90,
        APPROX_PERCENTILE(status_length_bucket, ARRAY(0.25, 0.5, 0.75)) AS status_length_bucket_quartiles,
        ROUND(AVG(status_length_bucket_2_filled), 2) AS avg_status_length_bucket_2_filled
    FROM base_subscriptions
    GROUP BY GROUPING SETS ((CASE WHEN status_length_bucket_2_filled >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN status_length >= 87 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN status_length_bucket_2 >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN status_length_bucket_2_filled >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN status_length >= 87 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN status_length_bucket_2_filled >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
)
SELECT
    status_length_bucket_2_filled_tier,
    status_length_bucket_quartiles,
    avg_status_length_bucket_2_filled
FROM stats
ORDER BY status_length_bucket_2_filled_tier, avg_status_length_bucket_2_filled
