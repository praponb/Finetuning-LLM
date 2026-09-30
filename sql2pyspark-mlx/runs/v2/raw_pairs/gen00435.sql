WITH ranked_store_sales AS (
    SELECT
        id,
        product_id,
        units,
        RANK() OVER (PARTITION BY product_id ORDER BY units DESC) AS rnk
    FROM store_sales
)
SELECT *
FROM ranked_store_sales
WHERE rnk <= 2;
