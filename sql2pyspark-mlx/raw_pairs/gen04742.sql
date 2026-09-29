WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', region, city) AS region_city_key,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM stores
    WHERE opened_date >= DATE '2023-05-11'
      AND region IS NOT NULL
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
    SUM(sqft)
    FOR city IN ('Singapore' AS singapore, 'Chiang Mai' AS chiang_mai)
);
