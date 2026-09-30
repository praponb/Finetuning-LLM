WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, REPLACE(name, ' ', '-') AS name_slug
    FROM customers
    WHERE segment IS NOT NULL
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.start_date <= s.signup_date
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        city,
        is_active,
        credit_limit,
        age,
        FIRST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY MONTH(signup_date), segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY YEAR(signup_date), city ORDER BY credit_limit, id) AS row_seq
    FROM not_flagged
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.monthly_fee > 130
        )
)
-- Grouped metrics
SELECT
    city,
    PERCENTILE_APPROX(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
    APPROX_PERCENTILE(first_known_credit_limit, 0.5) AS first_known_credit_limit_p50,
    SUM(CASE WHEN city = 'Singapore' THEN age ELSE 0 END) AS singapore_age,
    SUM(CASE WHEN city = 'Bangkok' THEN credit_limit ELSE 0 END) AS bangkok_credit_limit
FROM eligible
GROUP BY city
ORDER BY singapore_age;
