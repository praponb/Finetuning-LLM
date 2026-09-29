WITH valid_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM skus
    WHERE category IN ('Food', 'Beauty', 'Toys')
)
SELECT category, unit_cost
FROM valid_skus
UNION ALL
SELECT operator, unit_price
FROM stock_moves
WHERE operator IS NOT NULL
