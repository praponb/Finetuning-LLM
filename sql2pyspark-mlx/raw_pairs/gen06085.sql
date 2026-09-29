WITH base AS (
    -- Keep recent, valid rows
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
        credit_limit + age AS credit_limit_plus_age
    FROM customers
    WHERE signup_date > TO_DATE('2023-03-14')
      AND city IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(age / credit_limit, 2) AS age_per_credit_limit,
        COALESCE(city, 'NONE') AS city_clean
    FROM base
    WHERE signup_date >= DATE '2023-05-17'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.plan = 'standard'
        )
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT country
    FROM eligible
    INTERSECT
    SELECT country
    FROM customers
    WHERE country IN ('JP', 'US', 'UK')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    JOIN remaining_keys k ON s.country = k.country
)
-- Multi-level aggregation
SELECT
    city,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(DISTINCT id) AS distinct_ids,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list_2,
    COUNT_IF(country = 'DE') AS de_events,
    BIT_OR(age) AS age_bit_or
FROM reconciled
GROUP BY city
