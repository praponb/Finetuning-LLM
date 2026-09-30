WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty, id) AS pos
    FROM products
)
SELECT *
FROM numbered
WHERE pos <= 1;
