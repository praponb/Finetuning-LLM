WITH top_products AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty DESC, id) AS rnk
    FROM products
)
SELECT id, category, stock_qty
FROM top_products
WHERE rnk <= 1
ORDER BY stock_qty DESC, category DESC
