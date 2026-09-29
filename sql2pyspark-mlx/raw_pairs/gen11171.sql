SELECT id, store_id, sale_date
FROM (
    SELECT
        id,
        store_id,
        sale_date,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id) AS row_num
    FROM store_sales
) AS r
WHERE row_num <= 3;
