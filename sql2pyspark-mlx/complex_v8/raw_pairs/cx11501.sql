WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE move_type IS NOT NULL
      AND operator IN ('op_dew', 'op_eve', 'op_bee')
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count,
        (SELECT MAX(st.unit_price) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS max_move_unit_price,
        (SELECT COUNT(*) FILTER (WHERE st.move_type = 'TRANSFER') FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_move_type_count
    FROM cleaned s
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(move_count, 2) OVER (PARTITION BY sku_id, operator ORDER BY move_count ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_move_count,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY operator)) AS distinct_warehouse_id_in_operator,
        LEAD(unit_price) OVER w_sku_id_operator_by_move_coun_2 AS next_unit_price,
        MAX(unit_price) OVER w_operator AS operator_max_unit_price
    FROM with_stats
    WINDOW w_sku_id_operator_by_move_coun_2 AS (PARTITION BY sku_id, operator ORDER BY move_count ASC NULLS LAST, id), w_operator AS (PARTITION BY operator)
),
combined_details AS (
    -- Attach warehouse attributes
    SELECT s.*, w.city AS warehouse_city, w.capacity AS warehouse_capacity
    FROM metrics s
    JOIN warehouses w ON w.id = s.warehouse_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        max_move_unit_price,
        move_move_type_count,
        distinct_warehouse_id_in_operator,
        next_unit_price,
        operator_max_unit_price,
        warehouse_city,
        warehouse_capacity,
        MIN(move_move_type_count) OVER (PARTITION BY move_type, warehouse_id) AS move_type_warehouse_id_min_move_move_type_count,
        COUNT(*) OVER (PARTITION BY second_move_count, move_move_type_count ORDER BY move_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM combined_details
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.max_move_unit_price,
        s.move_move_type_count,
        s.distinct_warehouse_id_in_operator,
        s.next_unit_price,
        s.operator_max_unit_price,
        s.move_type_warehouse_id_min_move_move_type_count,
        s.running_count,
        w.capacity AS warehouse_capacity_2
    FROM windowed s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    moved_at,
    max_move_unit_price,
    move_move_type_count,
    distinct_warehouse_id_in_operator,
    next_unit_price,
    move_type_warehouse_id_min_move_move_type_count,
    running_count,
    warehouse_capacity_2,
    LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
    move_move_type_count - max_move_unit_price AS move_move_type_count_minus_max_move_unit_price
FROM with_parent
ORDER BY distinct_warehouse_id_in_operator DESC
