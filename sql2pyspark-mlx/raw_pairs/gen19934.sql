WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        FLOOR(sqft) sqft_whole,
        INITCAP(TRIM(region)) region_title,
        COALESCE(region, 'DIRECT') region_clean
    FROM stores
    WHERE MONTH(opened_date) IN (3, 5, 9)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT city, region
    FROM base
    INTERSECT
    SELECT city, region
    FROM stores
    WHERE UPPER(store_name) LIKE '%OWN%'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN matched_keys k ON s.city = k.city AND s.region = k.region
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock >= s.opened_date
        )
),
base_stores AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        sqft,
        sqft_whole,
        region_title,
        region_clean,
        DATEDIFF(CURRENT_DATE(), opened_date) / 365.25 years_since_opened_date,
        COALESCE(region, 'DIRECT') region_clean_2
    FROM not_flagged
    WHERE region IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.store_id IN (1, 2, 3, 4)
        )
)
-- Multi-level aggregation
SELECT
    city,
    PERCENTILE_APPROX(sqft_whole, 0.95) AS sqft_whole_p95,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    STDDEV_SAMP(sqft) AS sqft_stddev,
    COUNT_IF(city = 'Tokyo') AS tokyo_events
FROM kept
GROUP BY city
ORDER BY avg_sqft DESC;
