WITH top_store_sales AS (
    SELECT
        id,
        product_id,
        units,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) AS rn
    FROM store_sales
)
SELECT *
FROM top_store_sales
WHERE rn <= 3
ORDER BY units DESC;
