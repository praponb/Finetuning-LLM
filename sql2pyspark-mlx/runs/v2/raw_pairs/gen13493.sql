WITH Src AS (
    SELECT *
    FROM products
    WHERE stock_qty BETWEEN 263 AND 289
),
agg AS (
    SELECT category, MIN(stock_qty) AS min_stock_qty, SUM(cost) AS total_cost
    FROM Src
    GROUP BY category
)
SELECT category, min_stock_qty, total_cost
FROM agg
ORDER BY category;
