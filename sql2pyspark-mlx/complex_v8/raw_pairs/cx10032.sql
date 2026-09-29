WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price
    FROM stock_moves
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        unit_price,
        unit_price - LAG(unit_price) OVER w_operator_by_unit_price AS unit_price_change,
        DENSE_RANK() OVER w_operator_by_unit_price AS unit_price_dense_rank
    FROM valid_moves
    WINDOW w_operator_by_unit_price AS (PARTITION BY operator ORDER BY unit_price DESC, id)
),
first_rows AS (
    -- Top 3 per group
    SELECT *
    FROM metrics
    WHERE unit_price_dense_rank <= 3
)
-- Final projection
SELECT id, warehouse_id, unit_price_change
FROM first_rows
ORDER BY unit_price_change
