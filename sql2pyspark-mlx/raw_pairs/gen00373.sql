SELECT *
FROM (
    SELECT
        id,
        region,
        opened_date,
        sqft,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS row_num
    FROM stores
) AS ranked
WHERE row_num <= 3
