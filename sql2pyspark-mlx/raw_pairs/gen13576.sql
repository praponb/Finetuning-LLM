WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(email)) AS email_title,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM customers
    WHERE segment IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    id,
    name,
    email,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    email_title,
    AVG(credit_limit) OVER (PARTITION BY segment) AS segment_avg_credit_limit,
    AVG(age) OVER (PARTITION BY DATE_TRUNC('WEEK', signup_date), segment) AS signup_date_period_avg_age,
    SUM(age) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_age,
    CUME_DIST() OVER (ORDER BY age DESC NULLS LAST) AS age_cume_dist
FROM base
WHERE credit_limit <= 7900;
