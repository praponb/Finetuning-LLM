WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        moved_at + INTERVAL 30 MINUTES AS moved_at_plus_30m
    FROM stock_moves
    WHERE unit_price BETWEEN 73.0 AND 313.8
      AND qty > 145
),
enriched_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_filled,
        s.moved_at_plus_30m,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on
    FROM valid_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(moved_at_plus_30m, 'yyyy-MM') AS moved_at_plus_30m_ym,
        sku_id,
        MAX(moved_at_plus_30m) AS last_moved_at_plus_30m,
        SUM(CASE WHEN operator = 'op_ann' THEN 1 ELSE 0 END) AS n_operator_op_ann,
        COUNT(DISTINCT move_type) AS distinct_move_types,
        MIN(moved_at) AS first_moved_at,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        PERCENTILE_APPROX(unit_price_filled, 0.5) AS median_unit_price_filled
    FROM enriched_warehouse
    WHERE YEAR(warehouse_opened_on) = 2024
    GROUP BY DATE_FORMAT(moved_at_plus_30m, 'yyyy-MM'), sku_id
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.moved_at_plus_30m_ym,
        s.sku_id,
        s.last_moved_at_plus_30m,
        s.n_operator_op_ann,
        s.distinct_move_types,
        s.median_unit_price_filled,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.move_type,
        st.warehouse_id AS move_warehouse_id,
        st.moved_at AS move_moved_at
    FROM per_group s
    JOIN stock_moves st ON st.sku_id = s.sku_id AND st.warehouse_id IS NOT NULL
)
-- Final projection
SELECT
    moved_at_plus_30m_ym,
    sku_id,
    distinct_move_types,
    median_unit_price_filled,
    move_id,
    move_unit_price,
    move_type,
    move_warehouse_id,
    move_moved_at
FROM with_children_moves
ORDER BY distinct_move_types NULLS LAST, sku_id
