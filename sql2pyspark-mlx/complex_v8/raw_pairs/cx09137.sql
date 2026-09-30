WITH scoped AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        CONCAT_WS(' / ', move_type, operator) AS move_type_operator_label
    FROM stock_moves
    WHERE qty < 256
),
with_activity AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND LOWER(w.wh_code) LIKE '%bk%'
        )
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.operator AS move_operator
    FROM with_activity s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.move_id,
        st.id AS move_id_2,
        st.unit_price AS move_unit_price,
        st.qty AS move_qty,
        st.move_type AS move_move_type_2
    FROM line_level s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id AND st.unit_price < 330.0
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        ROUND(AVG(qty), 2) AS avg_move_qty,
        COUNT(CASE WHEN operator NOT IN ('op_eve', 'op_ann') THEN 1 END) AS move_operator_count
    FROM stock_moves
    GROUP BY sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.avg_move_qty,
        COALESCE(st.move_operator_count, 0) AS move_operator_count
    FROM with_children s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
latest AS (
    -- Keep the earliest row per sku_id, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        move_id,
        move_id_2,
        move_unit_price,
        move_qty,
        move_move_type_2,
        avg_move_qty,
        move_operator_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, move_type ORDER BY avg_move_qty, id, move_id, move_id_2) AS rn
        FROM with_stats
    ) ranked
    WHERE rn = 1
)
SELECT
    COALESCE(move_move_type_2, 'ALL') AS move_move_type_2,
    COALESCE(move_type, 'ALL') AS move_type,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(move_move_type_2)) AS move_move_type_2_list,
    ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list
FROM latest
GROUP BY GROUPING SETS ((move_move_type_2, move_type), (move_move_type_2), ())
