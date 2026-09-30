WITH filtered AS (
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
        YEAR(moved_at) AS moved_at_year
    FROM stock_moves
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count,
        (SELECT COUNT(*) FILTER (WHERE st.operator IN ('op_chan', 'op_ann', 'op_dew')) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_operator_count,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS last_moved_at
    FROM filtered s
),
move_stats_2 AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, MIN(moved_at) AS first_moved_at
    FROM stock_moves
    GROUP BY sku_id
),
with_stats_2 AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.first_moved_at
    FROM with_stats s
    LEFT JOIN move_stats_2 st ON st.sku_id = s.sku_id
),
with_details_warehouse AS (
    -- Join warehouses
    SELECT s.*, w.opened_on AS warehouse_opened_on, w.wh_code AS warehouse_wh_code
    FROM with_stats_2 s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_operator_count ORDER BY moved_at DESC), 3) AS moved_at_percent_rank,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id)) AS distinct_sku_id_in_warehouse_id,
        ROW_NUMBER() OVER (PARTITION BY move_operator_count ORDER BY moved_at DESC, id) AS rn,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id), 2) AS unit_price_vs_warehouse_id_avg,
        MAX(moved_at_year) OVER (PARTITION BY warehouse_id) AS warehouse_id_max_moved_at_year
    FROM with_details_warehouse
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    operator,
    unit_price,
    moved_at_year,
    move_count,
    move_operator_count,
    last_moved_at,
    first_moved_at,
    moved_at_percent_rank,
    rn,
    warehouse_id_max_moved_at_year,
    DATEDIFF(DATE '2025-06-30', last_moved_at) AS days_since_last_moved_at,
    ADD_MONTHS(warehouse_opened_on, 6) AS warehouse_opened_on_plus_6m
FROM windowed
ORDER BY warehouse_id_max_moved_at_year DESC NULLS FIRST, unit_price
