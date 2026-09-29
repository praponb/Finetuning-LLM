WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE operator IN ('op_eve', 'op_bee', 'op_chan')
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.sku_id AS move_sku_id,
        st.qty AS move_qty
    FROM filtered s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.sku_id <= 17
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price DESC, id, move_id) AS unit_price_seq,
        MIN(qty) OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price DESC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_qty_so_far,
        RANK() OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price DESC) AS unit_price_rank
    FROM with_children
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        move_id,
        move_moved_at,
        move_qty,
        unit_price_seq,
        min_qty_so_far,
        unit_price_rank,
        moved_at + INTERVAL 7 DAYS AS moved_at_plus_7d,
        HOUR(move_moved_at) AS move_moved_at_hour
    FROM windowed
)
SELECT id, move_id, measure, metric_value
FROM (
    SELECT id, move_id, move_moved_at_hour, unit_price_seq
    FROM annotated
) src
UNPIVOT (
    metric_value FOR measure IN (move_moved_at_hour, unit_price_seq)
) unp
