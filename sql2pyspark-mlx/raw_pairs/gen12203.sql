WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(credit_limit) OVER (PARTITION BY country ORDER BY age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_credit_limit,
        LAG(age, 1) OVER (ORDER BY signup_date ASC, id ASC) AS prev_age
    FROM customers
    WHERE signup_date <= '2025-02-18'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date >= s.signup_date
        )
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        prev_age,
        CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END AS is_chiang_mai,
        REPLACE(name, ' ', '') AS name_slug
    FROM kept
    WHERE prev_age BETWEEN 45 AND 49
),
base_customers AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(email)) AS email_title,
        INITCAP(TRIM(city)) AS city_title,
        COALESCE(city, 'UNKNOWN') AS resolved_city
    FROM base
    WHERE age IS NULL
      AND signup_date >= DATE '2023-09-02'
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base_customers s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND x.end_date <= s.signup_date
        )
)
-- Ranking and gap features
SELECT
    id,
    city,
    segment,
    age,
    prev_age,
    name_slug,
    city_title,
    resolved_city,
    NTILE(10) OVER (PARTITION BY country ORDER BY credit_limit DESC, id ASC) AS credit_limit_bucket,
    COUNT(prev_age) OVER (PARTITION BY city) AS city_count_prev_age
FROM eligible;
