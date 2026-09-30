WITH filtered_moves AS (
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
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
),
qualified AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM filtered_moves s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND w.opened_on BETWEEN DATE '2024-02-19' AND DATE '2025-02-14'
              AND LOWER(w.wh_code) LIKE '%kk%'
        )
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(moved_at, 3) OVER (PARTITION BY warehouse_id ORDER BY moved_at DESC, id) AS prev3_moved_at,
        LEAD(moved_at_hour) OVER (PARTITION BY operator_or_unknown ORDER BY unit_price DESC, id) AS next_moved_at_hour,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator_or_unknown ORDER BY unit_price DESC, id), 3) AS unit_price_percent_rank,
        NTH_VALUE(qty, 2) OVER (PARTITION BY warehouse_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        LAST_VALUE(moved_at_hour) IGNORE NULLS OVER (PARTITION BY warehouse_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at_hour
    FROM qualified
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY TO_DATE(moved_at), warehouse_id)) AS distinct_move_type_in_moved_at_to_date_warehouse,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY TO_DATE(moved_at), warehouse_id), 0), 4) AS qty_share_of_moved_at_to_date_warehouse_id
    FROM windowed
)
SELECT id, field, reading
FROM (
    SELECT id, unit_price_percent_rank, qty_share_of_moved_at_to_date_warehouse_id
    FROM metrics
) src
UNPIVOT (
    reading FOR field IN (unit_price_percent_rank AS m_unit_price_percent_rank, qty_share_of_moved_at_to_date_warehouse_id AS qty_share_of_moved_at_to_date_warehouse_id_v)
) unp
ORDER BY id, field
