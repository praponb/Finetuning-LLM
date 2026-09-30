WITH latest_match AS (
    SELECT
        s.*,
        latest.subscription_start_date,
        latest.end_date
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.start_date AS subscription_start_date, c.end_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.end_date <= s.signup_date
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.opened_date >= s.end_date
        )
),
metrics AS (
    SELECT
        *,
        NTILE(10) OVER (ORDER BY credit_limit DESC, id) AS credit_limit_bucket,
        AVG(credit_limit) OVER (PARTITION BY city) AS city_avg_credit_limit
    FROM kept
),
enriched AS (
    SELECT
        id,
        name,
        email,
        city,
        segment,
        age,
        subscription_start_date,
        end_date,
        credit_limit_bucket,
        city_avg_credit_limit,
        RANK() OVER (ORDER BY age NULLS FIRST) AS age_rank,
        LAG(city_avg_credit_limit, 1) OVER (PARTITION BY country ORDER BY end_date, id) AS prev_city_avg_credit_limit,
        LAST_VALUE(city_avg_credit_limit) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('YEAR', subscription_start_date) ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city_avg_credit_limit
    FROM metrics
    WHERE subscription_start_date >= '2025-10-01'
),
metrics_customers AS (
    SELECT
        id,
        name,
        email,
        age,
        end_date,
        credit_limit_bucket,
        city_avg_credit_limit,
        age_rank,
        prev_city_avg_credit_limit,
        ROUND(prev_city_avg_credit_limit / SUM(prev_city_avg_credit_limit) OVER (PARTITION BY city), 4) AS prev_city_avg_credit_limit_share,
        CUME_DIST() OVER (PARTITION BY city ORDER BY last_known_city_avg_credit_limit NULLS LAST) AS last_known_city_avg_credit_limit_cume_dist,
        LAST_VALUE(age, TRUE) OVER (ORDER BY prev_city_avg_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_age,
        LAG(credit_limit_bucket, 1) OVER (PARTITION BY DATE_TRUNC('week', subscription_start_date) ORDER BY credit_limit_bucket, id) AS prev_credit_limit_bucket,
        PERCENT_RANK() OVER (ORDER BY prev_city_avg_credit_limit) AS prev_city_avg_credit_limit_percent_rank
    FROM enriched
)
SELECT
    DATE_TRUNC('MONTH', end_date) AS activity_month,
    DATE_TRUNC('WEEK', end_date) AS week_start,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(prev_credit_limit_bucket, ARRAY(0.25, 0.5, 0.75)) AS prev_credit_limit_bucket_quartiles,
    PERCENTILE_APPROX(prev_city_avg_credit_limit, 0.9) AS prev_city_avg_credit_limit_p90,
    MAX(prev_city_avg_credit_limit_share) AS max_prev_city_avg_credit_limit_share,
    PERCENTILE_APPROX(last_known_city_avg_credit_limit_cume_dist, 0.9) AS last_known_city_avg_credit_limit_cume_dist_p90,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY PERCENTILE_APPROX(last_known_city_avg_credit_limit_cume_dist, 0.9) DESC) AS last_known_city_avg_credit_limit_cume_dist_p90_percent_rank
FROM metrics_customers
GROUP BY 1, 2
ORDER BY max_prev_city_avg_credit_limit_share DESC NULLS FIRST
