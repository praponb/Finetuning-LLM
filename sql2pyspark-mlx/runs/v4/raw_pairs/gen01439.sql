WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS tag
    FROM carts
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(tag) AS clean_tag,
        DAY(created_at) AS created_at_day
    FROM lines
    WHERE created_at >= DATE '2025-03-01'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(tag)) AS tag_title
    FROM valid
    WHERE customer_id IS NOT NULL
      AND HOUR(created_at) BETWEEN 6 AND 10
),
with_dims AS (
    -- Attach customers attributes
    SELECT s.*, p.email
    FROM cleaned s
    LEFT JOIN customers p
        ON s.customer_id = p.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN created_at_day >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END AS created_at_day_tier,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min_2,
    COUNT(CASE WHEN tag = 'WELCOME5' THEN 1 END) AS welcome5_count,
    ROUND(AVG(created_at_day), 2) AS avg_created_at_day
FROM with_dims
GROUP BY CASE WHEN created_at_day >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY duration_min_2 DESC NULLS FIRST;
