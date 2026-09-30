WITH numbered AS (
    SELECT
        id,
        category,
        stock_qty,
        cost,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty DESC, id) AS pos
    FROM products
    WHERE category IN ('Electronics', 'Books', 'Grocery')
)
SELECT
    id,
    category,
    stock_qty,
    cost
FROM numbered
WHERE pos <= 3
ORDER BY id DESC, cost DESC;
