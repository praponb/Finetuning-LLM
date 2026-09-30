WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), opened_date) / 30 AS months_since_opened_date,
        COALESCE(region, 'N/A') AS region_clean
    FROM stores
    WHERE region = 'South'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        region,
        sqft
    FROM valid
) src
PIVOT (
    SUM(sqft) AS amount,
    COUNT(1) AS item_count
    FOR region IN ('Central' AS central, 'East' AS east, 'South' AS south)
)
