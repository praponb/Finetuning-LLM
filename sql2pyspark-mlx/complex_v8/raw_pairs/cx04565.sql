WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-13 10:00:00'
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM valid_moves
    WHERE unit_price > (SELECT MIN(unit_price) FROM valid_moves)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        operator,
        MIN(moved_at) AS first_moved_at,
        COUNT(DISTINCT operator) AS distinct_operators
    FROM screened
    GROUP BY operator
    HAVING MAX(unit_price) > 130.6
)
-- Final projection
SELECT
    operator,
    distinct_operators,
    DATEDIFF(DATE '2025-12-31', first_moved_at) AS days_since_first_moved_at
FROM grouped
WHERE MONTH(first_moved_at) IN (2, 3)
