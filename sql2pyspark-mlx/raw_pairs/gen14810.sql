WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(sqft, 1) AS sqft_rounded
    FROM stores
    WHERE opened_date >= TO_DATE('2023-09-20')
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        city,
        region,
        sqft
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft_rounded DESC, id) AS row_num
        FROM cleaned
    ) x
    WHERE row_num = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(sqft AS STRING) AS sqft_str,
        COALESCE(region, 'OTHER') AS region_clean
    FROM deduped
    WHERE region IS NOT NULL
      AND region IS DISTINCT FROM 'North'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        region,
        sqft,
        LEAD(sqft) OVER (PARTITION BY city, region ORDER BY sqft, id) AS next_sqft,
        LAG(sqft) OVER (PARTITION BY city, region ORDER BY sqft, id) AS prev_sqft,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft, id) AS event_seq,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share
    FROM filtered
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.store_id = 3
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (ORDER BY sqft_share, id) AS sqft_share_bucket,
        NTILE(4) OVER (PARTITION BY city ORDER BY prev_sqft DESC, id) AS prev_sqft_bucket
    FROM not_flagged
    WHERE prev_sqft < 13279
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.product_id = 5
        )
)
-- Grouped metrics
SELECT
    region,
    COUNT_IF(region = 'Central') AS central_events,
    COUNT_IF(city = 'Berlin') AS berlin_events,
    SUM(CASE WHEN city = 'New York' THEN prev_sqft_bucket ELSE 0 END) AS new_york_prev_sqft_bucket,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SIZE(COLLECT_SET(city)) AS num_cities_2,
    GROUPING(region) AS gid
FROM eligible
GROUP BY CUBE (region)
ORDER BY central_events DESC NULLS FIRST
