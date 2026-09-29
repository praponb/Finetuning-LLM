WITH scoped AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price + qty AS unit_price_plus_qty
    FROM stock_moves
    WHERE (unit_price <= 323.9 OR operator = 'op_eve')
),
sku_id_stats AS (
    -- Avg qty per sku_id
    SELECT sku_id, AVG(qty) AS avg_qty
    FROM scoped
    GROUP BY sku_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_qty, ROUND(s.qty - g.avg_qty, 2) AS qty_minus_avg
    FROM scoped s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.qty > g.avg_qty
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(qty) OVER (PARTITION BY warehouse_id) AS warehouse_id_max_qty,
        ROUND(avg_qty / NULLIF(SUM(avg_qty) OVER (PARTITION BY warehouse_id), 0), 4) AS avg_qty_share_of_warehouse_id,
        FIRST_VALUE(avg_qty) IGNORE NULLS OVER (PARTITION BY sku_id, TO_DATE(moved_at) ORDER BY avg_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_avg_qty,
        LEAD(moved_at, 2) OVER (PARTITION BY sku_id, TO_DATE(moved_at) ORDER BY avg_qty, id) AS next2_moved_at
    FROM vs_group
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY moved_at), 3) AS moved_at_percent_rank,
        LAG(unit_price_plus_qty) OVER (ORDER BY qty_minus_avg DESC, id) AS prev_unit_price_plus_qty,
        LAG(qty_minus_avg) OVER (ORDER BY moved_at, id) AS prev_qty_minus_avg,
        ROUND(warehouse_id_max_qty / NULLIF(SUM(warehouse_id_max_qty) OVER (PARTITION BY TO_DATE(moved_at), warehouse_id), 0), 4) AS warehouse_id_max_qty_share_of_moved_at_to_date_w
    FROM windowed
),
annotated AS (
    -- Derive helper columns
    SELECT *, COALESCE(warehouse_id_max_qty, 0) AS warehouse_id_max_qty_filled
    FROM ranked
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        warehouse_id,
        sku_id,
        SUM(CASE WHEN move_type = 'INBOUND' THEN 1 ELSE 0 END) AS n_move_type_inbound,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list
    FROM annotated
    WHERE HOUR(next2_moved_at) BETWEEN 8 AND 12
    GROUP BY warehouse_id, sku_id
    HAVING MAX(qty) > 154
)
-- Final projection
SELECT warehouse_id, sku_id
FROM totals
ORDER BY sku_id DESC NULLS FIRST
