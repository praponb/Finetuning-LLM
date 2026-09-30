WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        CASE WHEN country = 'JP' THEN 1 ELSE 0 END AS is_jp
    FROM suppliers
    WHERE city IS NOT NULL
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.product_name LIKE 'La%'
        )
),
base_suppliers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        city,
        is_jp,
        TRIM(country) AS clean_country,
        COALESCE(city, 'N/A') AS resolved_city
    FROM eligible
    WHERE city IS DISTINCT FROM 'Tokyo'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY is_jp DESC NULLS LAST, id) AS row_num
    FROM base_suppliers
    QUALIFY row_num = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.supplier_id IS NOT NULL
        )
)
-- Grouped metrics
SELECT
    CASE WHEN is_jp >= 82 THEN 'HIGH' ELSE 'LOW' END AS is_jp_tier,
    SUM(CASE WHEN city = 'London' THEN is_jp ELSE 0 END) AS london_is_jp,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
    MAX(is_jp) AS max_is_jp,
    COUNT(*) AS event_count
FROM kept
GROUP BY CASE WHEN is_jp >= 82 THEN 'HIGH' ELSE 'LOW' END
