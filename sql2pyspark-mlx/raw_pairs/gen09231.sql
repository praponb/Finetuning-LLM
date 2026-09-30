WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', region, city) AS region_city_key,
        COALESCE(region, 'NONE') AS region_filled
    FROM stores
    WHERE region IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        region,
        sqft
    FROM base
) src
PIVOT (
    MAX(sqft)
    FOR region IN ('Central' AS central, 'West' AS west)
)
