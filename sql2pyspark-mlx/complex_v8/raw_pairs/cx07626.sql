WITH filtered_moves AS (
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
        SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix
    FROM stock_moves
    WHERE move_type IS NOT NULL
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN qty >= 275 THEN 'A' WHEN qty >= 123 THEN 'B' ELSE 'C' END AS qty_tier
    FROM filtered_moves
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
        )
),
with_parent AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.operator_prefix,
        s.qty_tier,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city
    FROM without_matches s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(warehouse_city) OVER w_sku_id_qty_tier) AS distinct_warehouse_city_in_sku_id_qty_tier,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id, qty_tier)) AS distinct_warehouse_id_in_sku_id_qty_tier,
        MAX(unit_price) OVER (PARTITION BY sku_id, move_type ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_so_far,
        ROUND(AVG(qty) OVER w_sku_id_qty_tier, 2) AS sku_id_qty_tier_avg_qty
    FROM with_parent
    WINDOW w_sku_id_qty_tier AS (PARTITION BY sku_id, qty_tier)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY operator ORDER BY sku_id_qty_tier_avg_qty ASC) AS sku_id_qty_tier_avg_qty_rank,
        ROUND(AVG(sku_id_qty_tier_avg_qty) OVER (PARTITION BY operator ORDER BY sku_id_qty_tier_avg_qty ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS sku_id_qty_tier_avg_qty_avg_4,
        LEAD(moved_at) OVER w_operator_by_sku_id_qty_tier_3 AS next_moved_at,
        LAG(sku_id_qty_tier_avg_qty) OVER w_operator_by_sku_id_qty_tier_3 AS prev_sku_id_qty_tier_avg_qty,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY distinct_warehouse_id_in_sku_id_qty_tier), 0), 4) AS qty_share_of_distinct_warehouse_id_in_sku_id_qty
    FROM with_trends
    WINDOW w_operator_by_sku_id_qty_tier_3 AS (PARTITION BY operator ORDER BY sku_id_qty_tier_avg_qty ASC, id), w_operator_by_sku_id_qty_tier AS (PARTITION BY operator ORDER BY sku_id_qty_tier_avg_qty ASC)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at,
    operator,
    qty_tier,
    warehouse_wh_code,
    warehouse_city,
    distinct_warehouse_city_in_sku_id_qty_tier,
    distinct_warehouse_id_in_sku_id_qty_tier,
    sku_id_qty_tier_avg_qty,
    sku_id_qty_tier_avg_qty_rank,
    sku_id_qty_tier_avg_qty_avg_4,
    next_moved_at,
    prev_sku_id_qty_tier_avg_qty,
    qty_share_of_distinct_warehouse_id_in_sku_id_qty,
    ROUND((UNIX_TIMESTAMP(next_moved_at) - UNIX_TIMESTAMP(moved_at)) / 3600, 1) AS hours_moved_at_to_next_moved_at
FROM with_windows
WHERE sku_id IS NOT NULL
