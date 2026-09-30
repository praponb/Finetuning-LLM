WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        INITCAP(TRIM(region)) AS region_title,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM stores
    WHERE region IS NOT NULL
      AND city = 'Bangkok'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        region,
        city,
        sqft
    FROM cleaned
) src
PIVOT (
    MAX(sqft) AS value_sum,
    COUNT(1) AS item_count
    FOR city IN ('Bangkok' AS bangkok, 'Chiang Mai' AS chiang_mai, 'Berlin' AS berlin)
);
