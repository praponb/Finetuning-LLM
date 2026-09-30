SELECT *
FROM (
    SELECT
        id,
        region,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS row_num
    FROM stores
) AS r
WHERE row_num <= 3;
