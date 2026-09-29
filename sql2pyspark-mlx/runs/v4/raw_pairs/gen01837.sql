SELECT id, store_id, sale_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date, id) AS row_num
    FROM store_sales
) AS ranked
WHERE row_num = 1;
