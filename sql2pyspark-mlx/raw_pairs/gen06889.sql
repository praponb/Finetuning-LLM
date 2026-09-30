WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        CONCAT_WS('-', supplier_name, country) AS supplier_name_country_key,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM suppliers
    WHERE rating > 2.5
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.stock_qty != 263
        )
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        country,
        supplier_name_country_key,
        INITCAP(TRIM(city)) AS city_title
    FROM not_flagged
    WHERE LENGTH(supplier_name) > 7
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, country
    FROM filtered
    INTERSECT
    SELECT id, country
    FROM suppliers
    WHERE city = 'New York'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.id = k.id AND s.country = k.country
)
-- Grouped metrics
SELECT
    country,
    COUNT_IF(country = 'DE') AS de_events,
    COUNT(*) FILTER (WHERE country = 'US') AS us_rows,
    COUNT(*) AS cnt,
    COUNT(DISTINCT country) AS distinct_countries,
    COUNT(*) AS row_count,
    COUNT_IF(country = 'DE') AS de_events_2,
    GROUPING_ID() AS grouping_id
FROM kept_rows
GROUP BY ROLLUP (country)
ORDER BY row_count DESC NULLS FIRST
