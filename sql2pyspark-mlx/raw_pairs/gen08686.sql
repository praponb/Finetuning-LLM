SELECT *
FROM (
    SELECT
        region,
        city,
        sqft
    FROM stores
) src
PIVOT (
    MAX(sqft) AS amount
    FOR city IN ('Chiang Mai' AS chiang_mai, 'Bangkok' AS bangkok)
);
