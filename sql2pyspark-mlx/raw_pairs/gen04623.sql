WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        credit_limit,
        INITCAP(TRIM(country)) AS country_title,
        COALESCE(city, 'UNKNOWN') AS resolved_city
    FROM customers
    WHERE segment IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit ASC NULLS FIRST, id ASC) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.customer_id IN (13, 35)
        )
),
base_customers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        segment,
        credit_limit,
        resolved_city,
        dedup_rank,
        REGEXP_REPLACE(email, ' ', '_') AS email_slug
    FROM kept
    WHERE city IS NOT NULL
      AND country = 'JP'
)
-- Window metrics over several different partitions
SELECT
    id,
    segment,
    credit_limit,
    dedup_rank,
    email_slug,
    COUNT(credit_limit) OVER w_ordered AS running_count_credit_limit,
    SUM(credit_limit) OVER w_ordered AS running_sum_credit_limit,
    AVG(credit_limit) OVER w_ordered AS running_avg_credit_limit,
    MAX(credit_limit) OVER w_ordered AS running_max_credit_limit
FROM base_customers
WINDOW w_ordered AS (PARTITION BY segment ORDER BY credit_limit ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
