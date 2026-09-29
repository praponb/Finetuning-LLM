WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN credit_limit >= 12800 THEN 'High' WHEN credit_limit >= 3000 THEN 'Medium' ELSE 'Low' END AS credit_limit_tier
    FROM customers
    WHERE name LIKE '%Olivi%'
),
with_latest AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.session_country,
        latest.browser
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.country AS session_country, c.browser
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY browser ORDER BY signup_date DESC NULLS FIRST, id) = 1
),
with_latest_customers AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.session_country_2
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.country AS session_country_2
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    BIT_AND(age) AS age_bit_and,
    APPROX_PERCENTILE(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
    ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN browser = 'Chrome' THEN 1 END), 0), 2) AS credit_limit_per_chrome
FROM with_latest_customers
GROUP BY activity_month
ORDER BY credit_limit_per_chrome DESC
