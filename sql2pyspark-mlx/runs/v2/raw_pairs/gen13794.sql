WITH top_store_sales AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY units DESC) AS row_num
    FROM store_sales
    WHERE revenue < 1300
)
SELECT
    id,
    store_id,
    units,
    sale_date
FROM top_store_sales
WHERE row_num <= 3;
