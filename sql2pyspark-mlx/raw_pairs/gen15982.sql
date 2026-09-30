WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS attr_key, v
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        v,
        EXPLODE(coupon_codes) AS tag
    FROM exploded
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(tag) AS clean_tag,
        COALESCE(tag, 'OTHER') AS tag_clean
    FROM lines
    WHERE tag IS NOT DISTINCT FROM 'WELCOME5'
      AND customer_id IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        v,
        tag,
        clean_tag,
        tag_clean,
        CASE WHEN tag = 'SPRING10' THEN 1 ELSE 0 END AS is_spring10,
        COALESCE(tag, 'OTHER') AS resolved_tag
    FROM base
    WHERE tag IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT a.*, c.email
    FROM valid a
    LEFT JOIN customers c
        ON a.customer_id <=> c.id
    WHERE c.city IN ('Tokyo', 'Singapore', 'New York')
)
-- Multi-level aggregation
SELECT
    tag,
    MIN(is_spring10) AS min_is_spring10,
    AVG(is_spring10) FILTER (WHERE tag = 'WELCOME5') AS welcome5_avg_is_spring10,
    GROUPING(tag) AS grouping_id
FROM joined
GROUP BY CUBE (tag);
