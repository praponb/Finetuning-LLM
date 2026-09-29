WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty, id) AS rn
    FROM products
)
SELECT id, category, stock_qty
FROM numbered
WHERE rn <= 5
