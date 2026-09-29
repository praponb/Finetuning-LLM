SELECT *
FROM (
    SELECT
        city,
        region,
        sqft
    FROM stores
) src
PIVOT (
    SUM(sqft) AS total
    FOR region IN ('North', 'East')
);
