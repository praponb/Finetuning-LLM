WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label,
        LEAST(unit_price, qty) AS least_unit_price_qty
    FROM stock_moves
    WHERE qty <= 426
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(qty) OVER (PARTITION BY warehouse_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY TO_DATE(moved_at), warehouse_id)) AS distinct_sku_id_in_moved_at_to_date_warehouse_id,
        LAG(least_unit_price_qty) OVER (PARTITION BY warehouse_id ORDER BY qty, id) AS prev_least_unit_price_qty,
        ROUND(qty / NULLIF(SUM(qty) OVER w_moved_at_to_date_warehouse_i, 0), 4) AS qty_share_of_moved_at_to_date_warehouse_id,
        MAX(qty) OVER (PARTITION BY TO_DATE(moved_at), warehouse_id) AS moved_at_to_date_warehouse_id_max_qty,
        LEAD(least_unit_price_qty, 3) OVER (PARTITION BY YEAR(moved_at), move_type ORDER BY qty NULLS LAST, id) AS next3_least_unit_price_qty
    FROM base
    WINDOW w_moved_at_to_date_warehouse_i AS (PARTITION BY TO_DATE(moved_at), warehouse_id)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(next3_least_unit_price_qty) OVER (PARTITION BY distinct_sku_id_in_moved_at_to_date_warehouse_id), 2) AS distinct_sku_id_in_moved_at_to_date_warehouse_id_2,
        ROUND(next3_least_unit_price_qty - AVG(next3_least_unit_price_qty) OVER w_distinct_sku_id_in_moved_at, 2) AS next3_least_unit_price_qty_vs_distinct_sku_id_in
    FROM metrics
    WINDOW w_distinct_sku_id_in_moved_at AS (PARTITION BY distinct_sku_id_in_moved_at_to_date_warehouse_id)
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS first_moved_at,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count_2
    FROM with_trends s
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM profiled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator IS NOT NULL
        )
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at,
    least_unit_price_qty,
    qty_share_of_moved_at_to_date_warehouse_id,
    moved_at_to_date_warehouse_id_max_qty,
    next3_least_unit_price_qty,
    distinct_sku_id_in_moved_at_to_date_warehouse_id_2,
    next3_least_unit_price_qty_vs_distinct_sku_id_in,
    move_count,
    move_count_2
FROM without_matches
WHERE sku_id IS NOT NULL
ORDER BY move_type DESC, sku_id NULLS LAST
