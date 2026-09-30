WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(is_active AS INT) AS is_active_flag,
        COALESCE(city, 'N/A') AS resolved_city
    FROM customers
    WHERE segment IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(is_active_flag) OVER (PARTITION BY MONTH(signup_date) ORDER BY signup_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS is_active_flag_avg_6,
        FIRST_VALUE(segment, TRUE) OVER (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_segment,
        SUM(credit_limit) OVER (PARTITION BY country ORDER BY is_active_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_credit_limit,
        CORR(age, credit_limit) OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date), segment) AS corr_age_credit_limit,
        STDDEV_SAMP(age) OVER (PARTITION BY city) AS city_stddev_samp_age
    FROM filtered
),
with_windows_customers AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_grp AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY is_active_flag, id) AS event_seq,
        LAG(is_active_flag_avg_6, 1) OVER w_part AS prev_is_active_flag_avg_6
    FROM with_windows
    WHERE city_stddev_samp_age = 65
    WINDOW w_grp AS (PARTITION BY segment ORDER BY city_stddev_samp_age DESC, id), w_part AS (PARTITION BY country, city ORDER BY is_active_flag, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(credit_limit, TRUE) OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit,
        SUM(corr_age_credit_limit) OVER (PARTITION BY city ORDER BY prev_is_active_flag_avg_6, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_corr_age_credit_limit
    FROM with_windows_customers
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(row_seq) OVER (PARTITION BY country, segment ORDER BY last_known_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_row_seq,
        MIN(row_seq) OVER (PARTITION BY country, segment ORDER BY last_known_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_row_seq,
        AVG(row_seq) OVER (PARTITION BY country, segment ORDER BY last_known_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_row_seq,
        FIRST_VALUE(is_active_flag_avg_6) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY city_stddev_samp_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_active_flag_avg_6
    FROM enriched
)
-- Grouped metrics
SELECT
    CASE WHEN running_min_row_seq >= 8.4 THEN 'HIGH' ELSE 'LOW' END AS running_min_row_seq_tier,
    segment,
    COUNT(*) FILTER (WHERE segment = 'Consumer') AS consumer_rows,
    AVG(credit_limit) FILTER (WHERE country = 'TH') AS th_avg_credit_limit
FROM metrics
GROUP BY 1, 2;
