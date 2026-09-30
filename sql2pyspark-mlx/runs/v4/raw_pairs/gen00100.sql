WITH valid AS (
    SELECT *, CONCAT(store_name, ' ', city) AS store_name_city
    FROM stores
    WHERE opened_date >= DATE '2023-11-14'
)
SELECT *
FROM (
    SELECT
        region,
        city,
        sqft
    FROM valid
) src
PIVOT (
    AVG(sqft) AS amount,
    COUNT(1) AS cnt
    FOR city IN ('Bangkok' AS bangkok, 'Chiang Mai' AS chiang_mai)
)
