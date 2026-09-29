WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        moved_at + INTERVAL 15 MINUTES AS moved_at_plus_15m,
        CONCAT(move_type, '#', CAST(sku_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 10 AND 12
),
totals AS (
    -- Summarise per group
    SELECT
        move_type,
        warehouse_id,
        sku_id,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(qty) / NULLIF(SUM(unit_price), 0), 3) AS qty_to_unit_price_ratio,
        SUM(CASE WHEN move_type = 'TRANSFER' THEN qty ELSE 0 END) AS qty_move_type_transfer,
        COUNT(*) FILTER (WHERE move_type IN ('TRANSFER', 'ADJUST')) AS n_move_type_in_transfer_adjust,
        COUNT(*) AS row_count
    FROM scoped_moves
    WHERE move_type IN ('OUTBOUND', 'ADJUST', 'INBOUND')
    GROUP BY 1, 2, 3
),
tagged AS (
    -- Add calculated columns
    SELECT
        move_type,
        warehouse_id,
        sku_id,
        distinct_ids,
        qty_to_unit_price_ratio,
        qty_move_type_transfer,
        row_count,
        ROUND(qty_to_unit_price_ratio * 0.9, 2) AS qty_to_unit_price_ratio_adj,
        LENGTH(TRIM(move_type)) AS move_type_len,
        CASE WHEN move_type IN ('ADJUST', 'TRANSFER') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM totals
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        move_type,
        warehouse_id,
        sku_id,
        distinct_ids,
        qty_to_unit_price_ratio,
        qty_move_type_transfer,
        qty_to_unit_price_ratio_adj,
        move_type_len,
        ROUND(PERCENT_RANK() OVER (PARTITION BY row_count ORDER BY distinct_ids DESC), 3) AS distinct_ids_percent_rank,
        MAX(qty_to_unit_price_ratio) OVER (PARTITION BY move_type_group, move_type) AS move_type_group_move_type_max_qty_to_unit_price_
    FROM tagged
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, MAX(moved_at) AS last_moved_at, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY warehouse_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at,
        COALESCE(st.move_count, 0) AS move_count
    FROM windowed s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(distinct_ids, 2) OVER (PARTITION BY move_count ORDER BY move_count DESC, move_type, warehouse_id, sku_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_ids,
        MIN(qty_move_type_transfer) OVER w_distinct_ids AS distinct_ids_min_qty_move_type_transfer,
        ROUND(move_count / NULLIF(SUM(move_count) OVER (PARTITION BY distinct_ids), 0), 4) AS move_count_share_of_distinct_ids
    FROM profiled
    WINDOW w_distinct_ids AS (PARTITION BY distinct_ids)
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.move_type,
        s.warehouse_id,
        s.sku_id,
        s.qty_to_unit_price_ratio,
        s.qty_move_type_transfer,
        s.last_moved_at,
        s.move_count_share_of_distinct_ids,
        st.id AS move_id,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at,
        st.operator AS move_operator
    FROM metrics s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.operator = 'op_bee'
)
SELECT
    move_operator,
    warehouse_id,
    GROUPING(move_operator) AS is_total_move_operator,
    ROUND(SUM(move_count_share_of_distinct_ids), 2) AS total_move_count_share_of_distinct_ids,
    MAX(qty_to_unit_price_ratio) AS max_qty_to_unit_price_ratio
FROM with_children_moves
GROUP BY ROLLUP(move_operator, warehouse_id)
ORDER BY move_operator NULLS LAST, warehouse_id NULLS LAST
