SELECT
    id,
    store_id,
    revenue,
    sale_date
FROM (
    SELECT
        id,
        store_id,
        revenue,
        sale_date,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS rn
    FROM store_sales
) AS t
WHERE rn <= 2;
