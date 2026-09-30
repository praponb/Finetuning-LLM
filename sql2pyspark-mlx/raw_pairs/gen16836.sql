WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('-', region, city) AS region_city_key
    FROM stores
    WHERE sqft > 5836
),
kept_keys AS (
    -- Set difference on keys
    SELECT region
    FROM cleaned
    EXCEPT
    SELECT region
    FROM stores
    WHERE opened_date >= '2025-01-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN kept_keys k ON s.region = k.region
),
cleaned_stores AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        store_name,
        opened_date,
        sqft,
        region_city_key,
        TRIM(store_name) AS clean_store_name
    FROM verified
    WHERE region IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        store_name,
        opened_date,
        sqft,
        clean_store_name,
        CUME_DIST() OVER (ORDER BY sqft DESC) AS sqft_cume_dist
    FROM cleaned_stores
    WHERE YEAR(opened_date) >= 2024
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    CASE WHEN sqft_cume_dist >= 0.68 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_cume_dist_tier,
    BIT_OR(sqft) AS sqft_bit_or,
    BIT_AND(sqft) AS sqft_bit_and,
    COUNT(*) AS row_count,
    BIT_AND(sqft) AS sqft_bit_and_2,
    GROUPING(DATE_TRUNC('YEAR', opened_date)) AS gid
FROM with_windows
GROUP BY CUBE (DATE_TRUNC('YEAR', opened_date), CASE WHEN sqft_cume_dist >= 0.68 THEN 'PREMIUM' ELSE 'STANDARD' END);
