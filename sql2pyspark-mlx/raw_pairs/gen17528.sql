WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(country, ', ', supplier_name) country_supplier_name,
        COALESCE(city, 'UNKNOWN') city_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        country_supplier_name,
        rank_in_group
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) = 1
),
latest_suppliers AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rank_in_group DESC, id) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.supplier_id IS NULL
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN rating >= 4.3 THEN 'LARGE' ELSE 'SMALL' END AS rating_tier,
    COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM kept
GROUP BY rating_tier
ORDER BY chiang_mai_count DESC NULLS LAST;
