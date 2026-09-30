WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        ABS(unit_price - 292.3) AS unit_price_distance,
        ROUND(unit_price * 1.07, 2) AS unit_price_adj,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE operator <> 'op_chan'
      AND unit_price <= 277.6
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        operator,
        unit_price_adj,
        COALESCE(unit_price_distance, 0.0) AS unit_price_distance_filled,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        ABS(unit_price_adj - 412.9) AS unit_price_adj_distance
    FROM scoped_moves
    WHERE operator IS NOT NULL
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.unit_price) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS min_move_unit_price,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count
    FROM enriched s
),
latest_per_key AS (
    -- Keep the latest row per move_type_or_unknown
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        operator,
        unit_price_adj,
        unit_price_distance_filled,
        move_type_or_unknown,
        unit_price_adj_distance,
        min_move_unit_price,
        move_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type_or_unknown ORDER BY move_count DESC, id DESC) AS recency_rank
        FROM profiled
    ) ranked
    WHERE recency_rank = 1
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.move_type,
        s.operator,
        s.unit_price_adj,
        s.unit_price_distance_filled,
        s.unit_price_adj_distance,
        s.min_move_unit_price,
        s.move_count,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.sku_id AS move_sku_id
    FROM latest_per_key s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    operator,
    unit_price_distance_filled,
    move_count,
    move_id,
    move_moved_at,
    move_sku_id,
    COALESCE(move_count, 0) AS move_count_filled,
    TO_DATE(move_moved_at) AS move_moved_at_date
FROM with_children
ORDER BY unit_price_distance_filled NULLS LAST, move_count_filled NULLS LAST
