WITH ranked_store_sales AS (
    SELECT
        id,
        product_id,
        units,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY units DESC) AS pos
    FROM store_sales
)
SELECT id, product_id, units
FROM ranked_store_sales
WHERE pos = 1;
