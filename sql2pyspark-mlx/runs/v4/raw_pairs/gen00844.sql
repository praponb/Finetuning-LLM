WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.plan <> 'standard'
        )
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(segment) AS segment_lower
    FROM eligible
    WHERE signup_date > DATE '2024-04-06'
      AND country IN ('TH', 'DE')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(credit_limit) OVER (ORDER BY signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit,
        AVG(credit_limit) OVER (ORDER BY age ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS credit_limit_avg_2
    FROM cleaned
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(running_credit_limit / SUM(running_credit_limit) OVER (PARTITION BY country), 4) AS running_credit_limit_share,
        SUM(age) OVER (ORDER BY age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age,
        ROUND(age / SUM(age) OVER (PARTITION BY city), 4) AS age_share
    FROM enriched
),
eligible_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.started_at <= s.signup_date
        )
),
enriched_customers AS (
    -- Ranking and gap features
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
        segment_lower,
        running_credit_limit,
        credit_limit_avg_2,
        running_credit_limit_share,
        running_age,
        age_share,
        NTILE(5) OVER (PARTITION BY city, country ORDER BY age_share DESC, id ASC) AS age_share_bucket,
        CUME_DIST() OVER (ORDER BY age ASC NULLS FIRST) AS age_cume_dist
    FROM eligible_customers
)
SELECT
    id,
    email,
    country,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    segment_lower,
    CASE WHEN country RLIKE '^(TH|US)$' THEN 'GROUP_B' WHEN country RLIKE '^(SG|JP)$' THEN 'EXTENDED' ELSE 'CORE' END AS country_zone
FROM enriched_customers
