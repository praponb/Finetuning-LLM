WITH top_store_sales AS (
    SELECT
        id,
        product_id,
        revenue,
        sale_date,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS pos
    FROM store_sales
)
SELECT
    id,
    product_id,
    revenue,
    sale_date
FROM top_store_sales
WHERE pos <= 3
ORDER BY id DESC;
