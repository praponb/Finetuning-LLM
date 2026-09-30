WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        CASE WHEN qty >= 300 THEN 'HIGH' WHEN qty >= 199 THEN 'MEDIUM' ELSE 'LOW' END AS qty_tier,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix
    FROM stock_moves
    WHERE unit_price > 200.7
      AND warehouse_id IS NOT NULL
),
deduped AS (
    -- Keep the latest row per operator, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        qty_tier,
        operator_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, sku_id ORDER BY qty DESC, id DESC) AS row_num
        FROM prepared_moves
    ) ranked
    WHERE row_num = 1
),
unique_rows AS (
    -- Keep the latest row per move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        qty_tier,
        operator_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY qty DESC, id DESC) AS rn
        FROM deduped
    ) ranked
    WHERE rn = 1
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS total_move_qty,
        (SELECT MAX(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS max_move_qty,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count
    FROM unique_rows s
),
joined_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.unit_price,
        s.qty_tier,
        s.operator_prefix,
        s.max_move_qty,
        sk.category AS sku_category,
        sk.is_active AS sku_is_active,
        sk.sku_code
    FROM profiled s
    JOIN skus sk ON sk.id = s.sku_id
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.sku_is_active,
        s.sku_code,
        st.id AS move_id,
        st.operator AS move_operator,
        st.moved_at AS move_moved_at,
        st.warehouse_id AS move_warehouse_id
    FROM joined_sku s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
-- Subtotals with GROUPING SETS
SELECT
    sku_is_active,
    warehouse_id,
    GROUPING(sku_is_active) AS is_total_sku_is_active,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE LOWER(sku_code) LIKE '%-7%') AS n_lower_sku_code_like_7
FROM exploded_detail_moves
GROUP BY GROUPING SETS ((sku_is_active, warehouse_id), (sku_is_active), (warehouse_id), ())
