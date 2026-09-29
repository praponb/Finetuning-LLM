SELECT *
FROM (
    SELECT
        id,
        region,
        sqft,
        city,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) rnk
    FROM stores
) r
WHERE rnk <= 5;
