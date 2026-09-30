SELECT *
FROM (
    SELECT
        city,
        region,
        sqft
    FROM stores
) src
PIVOT (
    SUM(sqft) AS amount
    FOR region IN ('Central' AS central, 'East' AS east)
);
