WITH cleaned AS (
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
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        QUARTER(moved_at) AS moved_at_quarter,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
    FROM stock_moves
    WHERE sku_id <= 2
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        move_type_slug,
        moved_at_quarter,
        moved_at_label,
        NTH_VALUE(qty, 2) OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        FIRST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty,
        ROUND(moved_at_quarter - AVG(moved_at_quarter) OVER (PARTITION BY sku_id), 2) AS moved_at_quarter_vs_sku_id_avg,
        ROW_NUMBER() OVER w_warehouse_id_move_type_by_mo_3 AS moved_at_quarter_seq,
        SUM(unit_price) OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price
    FROM cleaned
    WINDOW w_warehouse_id_move_type_by_mo_3 AS (PARTITION BY warehouse_id, move_type ORDER BY moved_at_quarter, id)
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(moved_at_quarter_seq - AVG(moved_at_quarter_seq) OVER w_moved_at_quarter_moved_at_qu, 2) AS moved_at_quarter_seq_vs_moved_at_quarter_moved_a,
        ROUND(AVG(running_unit_price) OVER (PARTITION BY moved_at_quarter, moved_at_quarter_seq), 2) AS moved_at_quarter_moved_at_quarter_seq_avg_runnin,
        MIN(qty) OVER (PARTITION BY moved_at_quarter, moved_at_quarter_seq) AS moved_at_quarter_moved_at_quarter_seq_min_qty,
        NTILE(3) OVER (PARTITION BY operator, moved_at_quarter_seq ORDER BY second_qty, id) AS second_qty_tercile,
        LAST_VALUE(moved_at_quarter_seq) OVER (PARTITION BY operator, moved_at_quarter_seq ORDER BY second_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at_quarter_seq
    FROM with_windows
    WINDOW w_moved_at_quarter_moved_at_qu AS (PARTITION BY moved_at_quarter, moved_at_quarter_seq)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT moved_at, second_qty_tercile, operator, moved_at_quarter
    FROM with_windows_2
) src
PIVOT (
    MAX(moved_at_quarter) AS max_v, COUNT(moved_at_quarter) AS n
    FOR operator IN ('op_chan', 'op_eve')
)
