SELECT
    id,
    city,
    opened_date,
    store_name
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS row_num
    FROM stores
) AS ranked
WHERE row_num <= 1;
