WITH ranked_store_sales AS (
    SELECT
        id,
        product_id,
        units,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) AS pos
    FROM store_sales
    WHERE sale_date > '2025-08-07'
)
SELECT id, product_id, units
FROM ranked_store_sales
WHERE pos <= 2
ORDER BY id DESC;
