WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(city)) AS city_title
    FROM customers
    WHERE signup_date > DATE '2023-09-17'
      AND city IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age DESC NULLS FIRST, id) AS dedup_rank
        FROM base
    ) ranked
    WHERE dedup_rank = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.status NOT IN ('EXPIRED', 'CANCELLED', 'PAUSED')
        )
),
deduped_customers AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date NULLS LAST, id) AS row_num
        FROM not_flagged
    ) t
    WHERE row_num = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY country, city ORDER BY dedup_rank DESC) AS latest_rank
    FROM deduped_customers
    QUALIFY latest_rank = 1
)
SELECT
    id,
    name,
    city,
    signup_date,
    is_active,
    credit_limit,
    city_title,
    dedup_rank,
    row_num
FROM ranked
ORDER BY city DESC;
