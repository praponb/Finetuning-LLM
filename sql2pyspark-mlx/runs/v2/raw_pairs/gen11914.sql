SELECT
    id,
    store_id,
    sale_date,
    revenue
FROM (
    SELECT
        id,
        store_id,
        sale_date,
        revenue,
        RANK() OVER (PARTITION BY store_id ORDER BY sale_date DESC) AS row_num
    FROM store_sales
) AS r
WHERE row_num <= 2;
