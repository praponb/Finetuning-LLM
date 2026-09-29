WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS element
    FROM carts
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM lines
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE created_at >= '2024-10-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    JOIN kept_keys k ON s.id = k.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN element IN ('WELCOME5', 'SPRING10') THEN 'Core' ELSE 'Other' END AS element_group
    FROM kept_rows
    WHERE element IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT e.*, c.segment
    FROM cleaned e
    LEFT JOIN customers c
        ON e.customer_id = c.id
)
SELECT
    id,
    customer_id,
    element,
    segment
FROM enriched
ORDER BY element DESC NULLS FIRST
