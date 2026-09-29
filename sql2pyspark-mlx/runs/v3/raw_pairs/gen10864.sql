WITH top_store_sales AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) AS row_num
    FROM store_sales
)
SELECT
    id,
    store_id,
    units,
    revenue
FROM top_store_sales
WHERE row_num <= 3
ORDER BY units DESC
