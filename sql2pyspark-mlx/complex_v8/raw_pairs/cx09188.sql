WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour,
        LENGTH(TRIM(operator)) AS operator_len,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE unit_price < 341.5
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY unit_price, id) AS unit_price_seq,
        DENSE_RANK() OVER (ORDER BY days_since_moved_at DESC) AS days_since_moved_at_dense_rank,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY operator_len)) AS distinct_move_type_in_operator_len,
        unit_price - LAG(unit_price) OVER (ORDER BY unit_price, id) AS unit_price_change
    FROM base
),
with_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.moved_at_hour,
        s.operator_len,
        s.days_since_moved_at,
        s.unit_price_seq,
        s.days_since_moved_at_dense_rank,
        s.distinct_move_type_in_operator_len,
        sk.category AS sku_category,
        sk.unit_cost AS sku_unit_cost
    FROM with_trends s
    JOIN skus sk ON sk.id = s.sku_id
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price_seq,
        s.distinct_move_type_in_operator_len,
        s.sku_unit_cost,
        st.id AS move_id,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at
    FROM with_details_sku s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        move_type,
        moved_at,
        unit_price_seq,
        distinct_move_type_in_operator_len,
        move_id,
        move_moved_at,
        ROUND(sku_unit_cost / NULLIF(unit_price_seq, 0), 3) AS sku_unit_cost_per_unit_price_seq,
        LOWER(TRIM(move_type)) AS move_type_clean
    FROM exploded_detail
),
with_activity AS (
    -- Keep rows whose move matches
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.id = s.move_id
              AND st.unit_price >= 215.4
              AND st.qty < 300
        )
),
enriched_move AS (
    -- Join stock_moves
    SELECT s.*, st.move_type AS move_move_type, st.warehouse_id AS move_warehouse_id
    FROM with_activity s
    JOIN stock_moves st ON st.id = s.move_id
)
-- Final projection
SELECT DISTINCT id, move_type
FROM enriched_move
ORDER BY move_type NULLS LAST, id
