WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(country) AS country_lower
    FROM customers
    WHERE city IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND YEAR(ch.opened_date) >= 2023
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        segment,
        signup_date,
        credit_limit,
        country_lower,
        FIRST_VALUE(credit_limit, TRUE) OVER w_recent AS first_known_credit_limit,
        LEAD(credit_limit, 1) OVER (PARTITION BY MONTH(signup_date), segment ORDER BY signup_date, id) AS next_credit_limit,
        LAST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit,
        LAG(credit_limit, 1, 0) OVER (PARTITION BY MONTH(signup_date), city ORDER BY age, id) AS prev_credit_limit
    FROM eligible
    WHERE email LIKE '%@outlook.com'
    WINDOW w_recent AS (PARTITION BY city, country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set difference on keys
    SELECT segment, id
    FROM scored
    EXCEPT
    SELECT segment, id
    FROM customers
    WHERE is_active = TRUE
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.segment = k.segment AND s.id = k.id
),
eligible_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND x.monthly_fee > 80
        )
),
matched_keys AS (
    -- Set intersection on keys
    SELECT segment
    FROM eligible_customers
    INTERSECT
    SELECT segment
    FROM customers
    WHERE LENGTH(email) >= 5
),
kept_rows_customers AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible_customers s
    INNER JOIN matched_keys k ON s.segment = k.segment
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        segment,
        country_lower,
        first_known_credit_limit,
        next_credit_limit,
        last_known_credit_limit,
        prev_credit_limit,
        SUBSTRING(city, 1, 3) AS city_prefix
    FROM kept_rows_customers
    WHERE next_credit_limit < 9500
      AND city IS NOT DISTINCT FROM 'Bangkok'
),
summary AS (
    -- Multi-level aggregation
    SELECT
        segment,
        SUM(CASE WHEN segment = 'Consumer' THEN last_known_credit_limit ELSE 0 END) AS consumer_last_known_credit_limit,
        COUNT(CASE WHEN segment = 'SMB' THEN 1 END) AS smb_count
    FROM filtered
    GROUP BY segment
)
SELECT
    segment,
    consumer_last_known_credit_limit,
    smb_count,
    CASE WHEN segment RLIKE '^(Corporate|SMB)$' THEN 'GROUP_B' WHEN segment RLIKE '^(Consumer)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS segment_zone
FROM summary
