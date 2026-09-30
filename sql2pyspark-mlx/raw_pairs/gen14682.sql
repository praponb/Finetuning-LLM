WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, LAST_DAY(signup_date) AS signup_date_month_end
    FROM customers
    WHERE signup_date >= TO_DATE('2023-03-17')
      AND city IS NOT NULL
),
with_last AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.session_started_at,
        x.session_browser
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.started_at AS session_started_at, c.browser AS session_browser
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
valid_customers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        country,
        signup_date,
        is_active,
        credit_limit,
        age,
        session_started_at,
        session_browser,
        COALESCE(name, 'N/A') AS name_filled,
        DATE_SUB(signup_date_month_end, 72) AS signup_date_month_end_minus_72d,
        COALESCE(session_browser, 'N/A') AS session_browser_filled
    FROM with_last
    WHERE signup_date > TO_DATE('2023-05-22')
      AND city IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid_customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND x.start_date >= s.session_started_at
        )
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    DATE_TRUNC('YEAR', session_started_at) AS year_start,
    CORR(credit_limit, age) AS corr_credit_limit_age,
    BIT_AND(age) AS age_bit_and,
    SIZE(COLLECT_SET(country)) AS num_countries,
    APPROX_PERCENTILE(credit_limit, 0.5) AS credit_limit_p50
FROM eligible
GROUP BY DATE_TRUNC('QUARTER', signup_date), DATE_TRUNC('YEAR', session_started_at)
HAVING age_bit_and > 1
