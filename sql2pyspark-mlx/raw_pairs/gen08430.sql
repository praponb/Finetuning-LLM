WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(plan)) AS plan_title
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_TRUNC('YEAR', start_date) AS start_date_period
    FROM valid
    WHERE end_date >= DATE '2023-09-27'
      AND customer_id IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        end_date,
        SUM(monthly_fee) OVER (PARTITION BY customer_id, status ORDER BY CAST(start_date_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS monthly_fee_sum_90d,
        SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY YEAR(start_date), customer_id ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
        FIRST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY plan ORDER BY start_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_3_prior
    FROM base
    WHERE YEAR(end_date) = 2024
),
base_subscriptions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        end_date,
        monthly_fee_sum_90d,
        running_monthly_fee,
        cumulative_monthly_fee,
        first_known_monthly_fee,
        monthly_fee_avg_3_prior,
        first_known_monthly_fee + cumulative_monthly_fee AS first_known_monthly_fee_plus_cumulative_monthly_fee
    FROM scored
    WHERE YEAR(end_date) = 2023
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        end_date,
        monthly_fee_sum_90d,
        first_known_monthly_fee,
        monthly_fee_avg_3_prior,
        MAX(first_known_monthly_fee) OVER (ORDER BY monthly_fee_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_first_known_monthly_fee,
        MIN(first_known_monthly_fee) OVER (ORDER BY monthly_fee_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_first_known_monthly_fee,
        AVG(first_known_monthly_fee) OVER (ORDER BY monthly_fee_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_first_known_monthly_fee,
        NTILE(10) OVER (PARTITION BY YEAR(end_date) ORDER BY first_known_monthly_fee, id) AS first_known_monthly_fee_bucket
    FROM base_subscriptions
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(monthly_fee_avg_3_prior AS DOUBLE) AS monthly_fee_avg_3_prior_num,
        CAST(running_min_first_known_monthly_fee AS INT) AS running_min_first_known_monthly_fee_int
    FROM enriched
    WHERE end_date > DATE '2023-10-25'
      AND first_known_monthly_fee <= 220
),
scored_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(monthly_fee_sum_90d, 1) OVER (PARTITION BY MONTH(end_date) ORDER BY monthly_fee_sum_90d, id) AS prev_monthly_fee_sum_90d,
        NTILE(4) OVER (ORDER BY first_known_monthly_fee, id) AS first_known_monthly_fee_bucket_2,
        LEAD(monthly_fee_avg_3_prior, 1) OVER (ORDER BY monthly_fee_avg_3_prior, id) AS next_monthly_fee_avg_3_prior
    FROM filtered
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN monthly_fee_avg_3_prior >= 192 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_avg_3_prior_tier,
        PERCENTILE_APPROX(running_min_first_known_monthly_fee_int, 0.5) AS running_min_first_known_monthly_fee_int_p50,
        PERCENTILE_APPROX(first_known_monthly_fee_bucket, ARRAY(0.25, 0.5, 0.75)) AS first_known_monthly_fee_bucket_quartiles,
        ROUND(AVG(running_avg_first_known_monthly_fee), 2) AS avg_running_avg_first_known_monthly_fee,
        ROUND(AVG(running_avg_first_known_monthly_fee), 2) AS avg_running_avg_first_known_monthly_fee_2,
        GROUPING(CASE WHEN monthly_fee_avg_3_prior >= 192 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
    FROM scored_subscriptions
    GROUP BY CUBE (CASE WHEN monthly_fee_avg_3_prior >= 192 THEN 'PREMIUM' ELSE 'STANDARD' END)
)
SELECT
    monthly_fee_avg_3_prior_tier,
    running_min_first_known_monthly_fee_int_p50,
    first_known_monthly_fee_bucket_quartiles,
    avg_running_avg_first_known_monthly_fee,
    avg_running_avg_first_known_monthly_fee_2,
    gid
FROM stats
