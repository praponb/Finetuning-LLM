WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM stock_moves
    WHERE operator IN ('op_dew', 'op_chan')
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        st.id AS move_id,
        st.sku_id AS move_sku_id,
        st.qty AS move_qty
    FROM scoped s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
warehouse_id_stats AS (
    -- Median move_qty per warehouse_id
    SELECT warehouse_id, PERCENTILE_APPROX(move_qty, 0.5) AS median_move_qty
    FROM detail_rows
    GROUP BY warehouse_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_move_qty,
        ROUND(s.move_qty - g.median_move_qty, 2) AS move_qty_minus_median
    FROM detail_rows s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.move_qty >= g.median_move_qty
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY operator ORDER BY median_move_qty) AS median_move_qty_dense_rank,
        MIN(median_move_qty) OVER (PARTITION BY operator ORDER BY median_move_qty, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_median_move_qty_so_far,
        LAG(move_qty, 2, 0) OVER (PARTITION BY operator ORDER BY median_move_qty, id, move_id) AS prev2_move_qty,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at)) AS rows_in_moved_at_month
    FROM vs_group
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT move_id, operator, move_qty
    FROM metrics
) src
PIVOT (
    MAX(move_qty) AS max_v, COUNT(move_qty) AS n
    FOR operator IN ('op_dew', 'op_bee', 'op_eve')
)
