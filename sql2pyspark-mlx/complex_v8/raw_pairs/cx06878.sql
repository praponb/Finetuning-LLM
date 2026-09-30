WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        CONCAT(operator, '#', CAST(sku_id AS STRING)) AS operator_ref,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
    WHERE sku_id <= 15
),
latest_per_key AS (
    -- Keep the latest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator_ref,
        unit_price_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY qty DESC, id DESC) AS recency_rank
        FROM scoped_moves
    ) ranked
    WHERE recency_rank = 1
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at
    FROM latest_per_key s
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS(': ', move_type, operator_ref) AS move_type_operator_ref_label
    FROM summarized_rows
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS first_moved_at,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count,
        (SELECT ROUND(AVG(st.qty), 2) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS avg_move_qty
    FROM labeled s
),
latest_per_key_2 AS (
    -- Keep the earliest row per sku_id, warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator_ref,
        unit_price_filled,
        last_moved_at,
        move_type_operator_ref_label,
        first_moved_at,
        move_count,
        avg_move_qty
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, warehouse_id ORDER BY avg_move_qty, id) AS recency_rank
        FROM profiled
    ) ranked
    WHERE recency_rank = 1
),
warehouse_id_stats AS (
    -- Max unit_price_filled per warehouse_id
    SELECT warehouse_id, MAX(unit_price_filled) AS max_unit_price_filled
    FROM latest_per_key_2
    GROUP BY warehouse_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_unit_price_filled,
        ROUND(s.unit_price_filled - g.max_unit_price_filled, 2) AS unit_price_filled_minus_max
    FROM latest_per_key_2 s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.unit_price_filled = g.max_unit_price_filled
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    qty,
    moved_at,
    unit_price_filled,
    last_moved_at,
    move_type_operator_ref_label,
    first_moved_at,
    avg_move_qty,
    unit_price_filled_minus_max
FROM vs_group
ORDER BY qty
