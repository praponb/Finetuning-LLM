WITH top_products AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY brand ORDER BY stock_qty DESC) AS rnk
    FROM products
)
SELECT
    id,
    brand,
    stock_qty,
    product_name
FROM top_products
WHERE rnk <= 3
