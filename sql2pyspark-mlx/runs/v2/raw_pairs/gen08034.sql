WITH top_store_sales AS (
    SELECT
        id,
        store_id,
        units,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) AS rnk
    FROM store_sales
    WHERE MONTH(sale_date) IN (2, 7, 8)
)
SELECT *
FROM top_store_sales
WHERE rnk <= 3
ORDER BY store_id, units DESC
