WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix,
        CONCAT(move_type, '#', CAST(warehouse_id AS STRING)) AS move_type_ref,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
    WHERE unit_price < 367.7
      AND sku_id <= 16
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        MIN(moved_at) AS first_moved_at,
        MAX(unit_price) AS max_move_unit_price
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.first_moved_at,
        st.max_move_unit_price
    FROM filtered s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(unit_price) OVER (PARTITION BY TO_DATE(moved_at)) AS moved_at_to_date_max_unit_price,
        RANK() OVER (PARTITION BY sku_id ORDER BY first_moved_at) AS first_moved_at_rank,
        NTILE(5) OVER w_sku_id_by_first_moved_at AS first_moved_at_quintile,
        NTILE(4) OVER (PARTITION BY MONTH(moved_at) ORDER BY moved_at DESC, id) AS moved_at_quartile,
        LAG(qty, 2) OVER w_sku_id_by_first_moved_at AS prev2_qty,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id ORDER BY first_moved_at, id), 3) AS first_moved_at_cume_dist
    FROM profiled
    WINDOW w_sku_id_by_first_moved_at AS (PARTITION BY sku_id ORDER BY first_moved_at, id)
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price,
        s.move_type_prefix,
        s.move_type_ref,
        s.first_moved_at,
        s.max_move_unit_price,
        s.first_moved_at_rank,
        s.first_moved_at_quintile,
        s.prev2_qty,
        st.id AS move_id,
        st.operator AS move_operator,
        st.sku_id AS move_sku_id,
        st.unit_price AS move_unit_price,
        st.qty AS move_qty
    FROM metrics s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
with_details_move AS (
    -- Attach move attributes
    SELECT
        s.*,
        st.sku_id AS move_sku_id_2,
        st.moved_at AS move_moved_at,
        st.move_type AS move_move_type,
        sk.labels AS sku_labels,
        sk.unit_cost AS sku_unit_cost
    FROM detail_rows s
    LEFT JOIN stock_moves st ON st.id = s.move_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
combined_details_sku AS (
    -- Join skus
    SELECT
        s.*,
        sk.unit_cost AS sku_unit_cost_2,
        sk.sku_code,
        sk.weight_kg AS sku_weight_kg,
        w.wh_code AS warehouse_wh_code
    FROM with_details_move s
    JOIN skus sk ON sk.id = s.move_sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    WHERE sk.unit_cost > 38.3
)
-- Final projection
SELECT
    id,
    sku_id,
    moved_at,
    unit_price,
    move_type_prefix,
    move_type_ref,
    first_moved_at,
    max_move_unit_price,
    first_moved_at_rank,
    prev2_qty,
    move_sku_id,
    move_unit_price,
    move_qty,
    move_sku_id_2,
    move_moved_at,
    move_move_type,
    sku_labels,
    sku_unit_cost,
    sku_unit_cost_2,
    sku_weight_kg,
    warehouse_wh_code
FROM combined_details_sku
ORDER BY move_sku_id, moved_at
