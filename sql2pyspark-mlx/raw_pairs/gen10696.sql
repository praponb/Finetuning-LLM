WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(city) AS city_lower,
        DATEDIFF(CURRENT_DATE(), opened_date) / 365 AS years_since_opened_date,
        COALESCE(region, 'NONE') AS region_filled
    FROM stores
    WHERE opened_date >= DATE '2023-08-06'
      AND city <> 'Berlin'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.units IS NOT NULL
        )
),
kept_keys AS (
    -- Set difference on keys
    SELECT city
    FROM not_flagged
    EXCEPT ALL
    SELECT city
    FROM stores
    WHERE city <> 'Tokyo'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    INNER JOIN kept_keys k ON s.city = k.city
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(years_since_opened_date / sqft, 2) AS years_since_opened_date_per_sqft
    FROM reconciled
    WHERE region IS NOT DISTINCT FROM 'East'
)
-- Grouped metrics
SELECT
    city,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    SIZE(COLLECT_SET(city)) AS num_cities,
    ROUND(AVG(years_since_opened_date_per_sqft), 2) AS avg_years_since_opened_date_per_sqft,
    AVG(years_since_opened_date) AS avg_years_since_opened_date,
    COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM valid
GROUP BY GROUPING SETS ((city, DATE_TRUNC('QUARTER', opened_date)), (city));
