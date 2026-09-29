WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN move_type IN ('ADJUST', 'TRANSFER') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE qty >= 154
),
qualified AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND w.capacity < 11768
              AND w.opened_on < DATE '2025-07-03'
        )
),
joined_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.move_type_group,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code
    FROM qualified s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE w.opened_on BETWEEN DATE '2024-04-13' AND DATE '2025-07-03'
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM joined_warehouse s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator = 'op_bee'
              AND st.qty < 288
        )
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        warehouse_opened_on,
        LAST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY unit_price DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        NTILE(10) OVER (PARTITION BY sku_id ORDER BY warehouse_opened_on, id) AS warehouse_opened_on_decile,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY warehouse_opened_on, id) AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id, MONTH(moved_at) ORDER BY unit_price DESC, id), 3) AS unit_price_cume_dist,
        ROUND(CUME_DIST() OVER w_sku_id_by_warehouse_opened_o, 3) AS warehouse_opened_on_cume_dist,
        LAG(moved_at) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY unit_price DESC NULLS FIRST, id) AS prev_moved_at
    FROM without_matches
    WINDOW w_sku_id_by_warehouse_opened_o AS (PARTITION BY sku_id ORDER BY warehouse_opened_on, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        qty,
        last_known_qty,
        warehouse_opened_on_decile,
        rn,
        unit_price_cume_dist,
        prev_moved_at,
        warehouse_opened_on_decile - LAG(warehouse_opened_on_decile) OVER (PARTITION BY rn, sku_id ORDER BY prev_moved_at ASC, id) AS warehouse_opened_on_decile_change,
        COUNT(*) OVER (PARTITION BY warehouse_opened_on_decile) AS rows_in_warehouse_opened_on_decile,
        DATEDIFF(warehouse_opened_on, LAG(warehouse_opened_on) OVER w_warehouse_opened_on_decile_p) AS days_since_prev_warehouse_opened_on,
        LEAD(qty, 2) OVER (PARTITION BY warehouse_opened_on_decile, DATE_TRUNC('MONTH', prev_moved_at) ORDER BY prev_moved_at DESC, id) AS next2_qty,
        ROUND(CUME_DIST() OVER (PARTITION BY rn, sku_id ORDER BY prev_moved_at ASC), 3) AS prev_moved_at_cume_dist
    FROM with_windows
    WINDOW w_warehouse_opened_on_decile_p AS (PARTITION BY warehouse_opened_on_decile, DATE_TRUNC('MONTH', prev_moved_at) ORDER BY prev_moved_at DESC, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, prev_moved_at_cume_dist, unit_price_cume_dist
    FROM metrics
) src
UNPIVOT (
    value FOR field IN (prev_moved_at_cume_dist AS m_prev_moved_at_cume_dist, unit_price_cume_dist AS unit_price_cume_dist_v)
) unp
ORDER BY id, field
