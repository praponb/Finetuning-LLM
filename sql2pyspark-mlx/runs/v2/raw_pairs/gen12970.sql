SELECT
    id,
    store_id,
    sale_date,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date, id) AS rn
FROM store_sales
WHERE units >= 19;
