WITH ranked_store_sales AS (
    SELECT
        id,
        product_id,
        units,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) AS rn
    FROM store_sales
)
SELECT *
FROM ranked_store_sales
WHERE rn = 1;
