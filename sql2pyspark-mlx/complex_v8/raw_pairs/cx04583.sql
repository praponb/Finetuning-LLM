WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd') AS moved_at_label,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.operator <> 'op_bee') FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_operator_count,
        (SELECT ROUND(AVG(st.unit_price), 2) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS avg_move_unit_price,
        (SELECT MIN(st.qty) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS min_move_qty
    FROM valid_moves s
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_sku_id_operator_by_unit_pric AS rn,
        MIN(unit_price) OVER (PARTITION BY move_operator_count, sku_id) AS move_operator_count_sku_id_min_unit_price,
        ROUND(AVG(unit_price) OVER w_move_operator_count_sku_id, 2) AS move_operator_count_sku_id_avg_unit_price,
        MAX(avg_move_unit_price) OVER (PARTITION BY move_operator_count, sku_id) AS move_operator_count_sku_id_max_avg_move_unit_pri,
        ROUND(min_move_qty / NULLIF(SUM(min_move_qty) OVER (PARTITION BY move_operator_count, sku_id), 0), 4) AS min_move_qty_share_of_move_operator_count_sku_id
    FROM summarized_rows
    WINDOW w_sku_id_operator_by_unit_pric AS (PARTITION BY sku_id, operator ORDER BY unit_price_bucket DESC, id), w_move_operator_count_sku_id AS (PARTITION BY move_operator_count, sku_id)
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.moved_at_label,
        s.move_operator_count,
        s.avg_move_unit_price,
        s.rn,
        s.min_move_qty_share_of_move_operator_count_sku_id,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.move_type,
        st.warehouse_id AS move_warehouse_id,
        st.moved_at AS move_moved_at
    FROM with_trends s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
unique_rows AS (
    -- Keep the earliest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        moved_at_label,
        move_operator_count,
        avg_move_unit_price,
        rn,
        min_move_qty_share_of_move_operator_count_sku_id,
        move_id,
        move_unit_price,
        move_type,
        move_warehouse_id,
        move_moved_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY avg_move_unit_price, id, move_id) AS recency_rank
        FROM with_children_moves
    ) ranked
    WHERE recency_rank = 1
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS total_move_qty
    FROM unique_rows s
)
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    operator,
    moved_at_label,
    rn,
    min_move_qty_share_of_move_operator_count_sku_id,
    move_unit_price,
    move_type,
    move_moved_at
FROM with_counts
ORDER BY warehouse_id, move_type DESC
