WITH ranked AS (
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY age NULLS LAST) AS row_num
    FROM customers
    QUALIFY row_num = 1
),
with_windows AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY segment ORDER BY credit_limit, id) credit_limit_bucket,
        CORR(age, credit_limit) OVER (PARTITION BY segment) corr_age_credit_limit,
        NTILE(3) OVER (PARTITION BY segment, country ORDER BY credit_limit, id) credit_limit_bucket_2,
        ROUND(age / SUM(age) OVER (PARTITION BY country), 4) age_share
    FROM ranked
),
metrics AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        row_num,
        credit_limit_bucket,
        corr_age_credit_limit,
        credit_limit_bucket_2,
        age_share,
        FIRST_VALUE(credit_limit_bucket, TRUE) OVER (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_credit_limit_bucket,
        CUME_DIST() OVER (PARTITION BY city, country ORDER BY corr_age_credit_limit DESC) corr_age_credit_limit_cume_dist,
        ROUND(credit_limit_bucket / SUM(credit_limit_bucket) OVER (PARTITION BY country), 4) credit_limit_bucket_share
    FROM with_windows
)
SELECT
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    segment,
    COUNT_IF(country = 'US') AS us_events,
    BIT_AND(age) AS age_bit_and,
    COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count,
    ROUND(SUM(credit_limit_bucket) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS credit_limit_bucket_per_singapore
FROM metrics
GROUP BY CUBE (DATE_TRUNC('QUARTER', signup_date), segment)
ORDER BY credit_limit_bucket_per_singapore;
