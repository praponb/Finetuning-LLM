WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        CASE WHEN region IN ('North', 'Central') THEN 'Group A' ELSE 'Other' END AS region_group,
        CONCAT_WS('-', city, region) AS city_region_key
    FROM stores
    WHERE opened_date <= '2025-01-13'
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.units BETWEEN 17 AND 31
        )
),
not_flagged_stores AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.product_id = 6
        )
),
base_stores AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        region_group,
        city_region_key,
        SUBSTRING(region, 1, 2) AS region_prefix,
        ROUND(sqft * 1.1, 2) AS sqft_scaled
    FROM not_flagged_stores
    WHERE sqft < 15517
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        region_group,
        city_region_key,
        region_prefix,
        sqft_scaled,
        UPPER(region) AS region_upper,
        COALESCE(region, 'OTHER') AS region_filled
    FROM base_stores
    WHERE region IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(sqft_scaled, 1) OVER (PARTITION BY region ORDER BY sqft_scaled, id) AS next_sqft_scaled,
        FIRST_VALUE(region) IGNORE NULLS OVER (ORDER BY sqft_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region
    FROM valid
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY next_sqft_scaled NULLS FIRST, id) AS rank_in_group
    FROM with_windows
    QUALIFY rank_in_group = 1
)
-- Aggregate with conditional metrics
SELECT
    region,
    city,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(next_sqft_scaled, ARRAY(0.25, 0.5, 0.75)) AS next_sqft_scaled_quartiles,
    PERCENTILE_APPROX(sqft_scaled, ARRAY(0.25, 0.5, 0.75)) AS sqft_scaled_quartiles,
    AVG(sqft_scaled) AS avg_sqft_scaled,
    COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count
FROM latest
GROUP BY region, city
ORDER BY event_count DESC NULLS FIRST;
