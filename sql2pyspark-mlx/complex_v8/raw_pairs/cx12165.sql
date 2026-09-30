WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        FLOOR(capacity / 5000) * 5000 AS capacity_bucket
    FROM warehouses
    WHERE city = 'Kuala Lumpur'
),
derived AS (
    -- Derive helper columns
    SELECT *, DATE_TRUNC('QUARTER', opened_on) AS opened_on_quarter
    FROM recent
    WHERE capacity_bucket >= 2900
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        ROUND(AVG(qty), 2) AS avg_move_qty,
        MAX(qty) AS max_move_qty
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.avg_move_qty,
        st.max_move_qty
    FROM derived s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, max_move_qty, capacity, capacity_bucket
    FROM summarized_rows
) src
UNPIVOT (
    metric_value FOR measure IN (max_move_qty, capacity, capacity_bucket)
) unp
