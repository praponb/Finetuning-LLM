WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket,
        CONCAT_WS('-', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE operator NOT IN ('op_chan', 'op_dew')
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        MAX(moved_at) AS last_moved_at,
        COUNT(*) AS move_count,
        COUNT(*) FILTER (WHERE operator IS NOT NULL) AS move_operator_count
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_operator_count, 0) AS move_operator_count
    FROM filtered s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM summarized_rows s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.unit_price <= 323.9
              AND MONTH(st.moved_at) IN (1, 2)
        )
),
joined AS (
    -- Attach warehouse attributes
    SELECT
        s.*,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code,
        sk.sku_code
    FROM with_activity s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
unique_rows AS (
    -- Keep the latest row per warehouse_city
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        unit_price_bucket,
        operator_move_type_label,
        last_moved_at,
        move_count,
        move_operator_count,
        warehouse_city,
        warehouse_opened_on,
        warehouse_wh_code,
        sku_code
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_city ORDER BY move_operator_count DESC, id DESC) AS row_num
        FROM joined
    ) ranked
    WHERE row_num = 1
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        sku_id,
        move_operator_count,
        SUM(move_count) AS total_move_count,
        MAX(move_operator_count) AS max_move_operator_count,
        PERCENTILE_APPROX(unit_price, 0.5) AS median_unit_price,
        ROUND(AVG(qty), 2) AS avg_qty
    FROM unique_rows
    WHERE warehouse_city IN ('Bangkok', 'Singapore')
    GROUP BY sku_id, move_operator_count
)
-- Final projection
SELECT
    sku_id,
    move_operator_count,
    total_move_count,
    max_move_operator_count,
    median_unit_price,
    avg_qty,
    ABS(median_unit_price - 225.6) AS median_unit_price_distance
FROM per_group
ORDER BY total_move_count, sku_id, move_operator_count
LIMIT 5
