WITH not_flagged AS (
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.order_date >= s.opened_date
        )
),
kept_keys AS (
    SELECT region
    FROM not_flagged
    INTERSECT ALL
    SELECT region
    FROM stores
    WHERE region = 'East'
),
kept_rows AS (
    SELECT s.*
    FROM not_flagged s
    JOIN kept_keys k ON s.region = k.region
),
valid AS (
    SELECT
        *,
        CEIL(sqft) sqft_whole,
        REGEXP_REPLACE(store_name, ' ', '') store_name_slug
    FROM kept_rows
    WHERE opened_date >= DATE '2023-05-20'
      AND opened_date <= '2023-11-16'
),
filtered AS (
    SELECT *, sqft_whole + sqft sqft_whole_plus_sqft
    FROM valid
    WHERE opened_date >= '2024-10-12'
),
remaining_keys AS (
    SELECT region, city
    FROM filtered
    INTERSECT
    SELECT region, city
    FROM stores
    WHERE region = 'West'
),
kept_rows_stores AS (
    SELECT s.*
    FROM filtered s
    JOIN remaining_keys k ON s.region = k.region AND s.city = k.city
)
SELECT
    city,
    APPROX_PERCENTILE(sqft_whole, 0.95) AS sqft_whole_p95,
    SUM(CASE WHEN city = 'Berlin' THEN sqft_whole_plus_sqft ELSE 0 END) AS berlin_sqft_whole_plus_sqft,
    SIZE(COLLECT_SET(region)) AS num_regions,
    SIZE(COLLECT_SET(region)) AS num_regions_2,
    COUNT(CASE WHEN region = 'South' THEN 1 END) AS south_count
FROM kept_rows_stores
GROUP BY city
