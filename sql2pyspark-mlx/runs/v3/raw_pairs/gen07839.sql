SELECT *
FROM (
    SELECT
        id,
        city,
        opened_date,
        sqft,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY opened_date DESC) AS rnk
    FROM stores
) AS ranked
WHERE rnk <= 2;
