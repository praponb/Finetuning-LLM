WITH recent_moves AS (
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
        qty + unit_price AS qty_plus_unit_price,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix,
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND operator IN ('op_chan', 'op_dew', 'op_eve')
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.sku_id AS move_sku_id,
        st.unit_price AS move_unit_price,
        st.operator AS move_operator
    FROM recent_moves s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.sku_id <= 15
),
combined_details_move AS (
    -- Attach move attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_id,
        s.move_move_type,
        s.move_sku_id,
        s.move_operator,
        st.sku_id AS move_sku_id_2
    FROM exploded_detail_moves s
    JOIN stock_moves st ON st.id = s.move_id
),
combined_details AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price,
        s.move_id,
        s.move_move_type,
        s.move_sku_id,
        s.move_sku_id_2,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg,
        sk.is_active AS sku_is_active
    FROM combined_details_move s
    LEFT JOIN skus sk ON sk.id = s.move_sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.qty) FROM stock_moves st WHERE st.sku_id = s.move_sku_id) AS min_move_qty,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.move_sku_id) AS move_count,
        (SELECT ROUND(AVG(st.unit_price), 2) FROM stock_moves st WHERE st.sku_id = s.move_sku_id) AS avg_move_unit_price
    FROM combined_details s
),
rollup_base AS (
    -- Group-level totals
    SELECT
        move_sku_id,
        move_count,
        move_type,
        SUM(min_move_qty) AS total_min_move_qty,
        SUM(CASE WHEN move_type = 'OUTBOUND' THEN 1 ELSE 0 END) AS n_move_type_outbound,
        SUM(CASE WHEN move_type IS NOT NULL THEN 1 ELSE 0 END) AS n_move_type_is_not_null,
        SUM(CASE WHEN sku_category = 'Electronics' THEN 1 ELSE 0 END) AS n_sku_category_electronics,
        ROUND(SUM(move_count) / NULLIF(SUM(unit_price), 0), 3) AS move_count_to_unit_price_ratio,
        COUNT(*) AS row_count
    FROM profiled
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        move_sku_id,
        move_count,
        move_type,
        n_move_type_outbound,
        n_move_type_is_not_null,
        n_sku_category_electronics,
        row_count,
        MIN(n_move_type_is_not_null) OVER (PARTITION BY row_count, move_sku_id) AS row_count_move_sku_id_min_n_move_type_is_not_nul,
        MAX(move_count) OVER (PARTITION BY row_count, move_sku_id) AS row_count_move_sku_id_max_move_count,
        SUM(n_sku_category_electronics) OVER (ORDER BY row_count DESC, move_sku_id, move_count, move_type ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS n_sku_category_electronics_sum_1_prior,
        MIN(move_count) OVER (PARTITION BY row_count, move_sku_id) AS row_count_move_sku_id_min_move_count,
        COUNT(*) OVER (PARTITION BY row_count, move_sku_id) AS rows_in_row_count_move_sku_id,
        LAG(n_move_type_is_not_null) OVER (PARTITION BY move_type ORDER BY n_move_type_is_not_null, move_sku_id, move_count, move_type) AS prev_n_move_type_is_not_null
    FROM rollup_base
)
SELECT *
FROM (
    SELECT n_move_type_outbound, move_type, prev_n_move_type_is_not_null
    FROM with_windows
) src
PIVOT (
    SUM(prev_n_move_type_is_not_null)
    FOR move_type IN ('INBOUND', 'ADJUST')
)
