WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        LOWER(country) country_lower
    FROM suppliers
    WHERE country = 'US'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY city, country ORDER BY rating DESC) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(dedup_rank AS STRING) dedup_rank_num,
        SUBSTRING(supplier_name, 1, 2) supplier_name_prefix
    FROM latest
    WHERE city IN ('Bangkok', 'Tokyo', 'Chiang Mai')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating DESC NULLS FIRST, id) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND ARRAY_CONTAINS(x.tags, 'clearance')
        )
)
SELECT
    id,
    supplier_name,
    country,
    rating,
    dedup_rank,
    supplier_name_prefix,
    latest_rank,
    CASE WHEN country RLIKE '^(US|JP)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS country_zone
FROM not_flagged
ORDER BY id
LIMIT 5
