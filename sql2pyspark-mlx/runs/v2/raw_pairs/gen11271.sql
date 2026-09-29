WITH top_products AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY supplier_id ORDER BY price) AS rnk
    FROM products
)
SELECT id, supplier_id, price
FROM top_products
WHERE rnk = 1
