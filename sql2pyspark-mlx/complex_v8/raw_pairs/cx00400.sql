WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        LENGTH(TRIM(operator)) AS operator_len,
        CONCAT(move_type, '#', CAST(warehouse_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-25 07:00:00'
      AND operator IN ('op_chan', 'op_ann')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(operator_len) OVER (PARTITION BY move_type) AS move_type_min_operator_len,
        LAST_VALUE(qty) OVER (PARTITION BY move_type ORDER BY unit_price DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        ROUND(operator_len - AVG(operator_len) OVER (PARTITION BY move_type), 2) AS operator_len_vs_move_type_avg,
        ROUND(AVG(qty) OVER (PARTITION BY operator, warehouse_id ORDER BY operator_len, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_2_prior,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type ORDER BY unit_price DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_7,
        NTH_VALUE(qty, 2) OVER (PARTITION BY operator, warehouse_id ORDER BY operator_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty
    FROM filtered_moves
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(unit_price_avg_7) IGNORE NULLS OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_avg_7,
        FIRST_VALUE(second_qty) IGNORE NULLS OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_second_qty,
        NTILE(3) OVER (ORDER BY qty, id) AS qty_tercile,
        ROUND(AVG(qty_avg_2_prior) OVER (ORDER BY qty, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_2_prior_avg_2_prior
    FROM windowed
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.move_type_min_operator_len,
        s.first_known_unit_price_avg_7,
        s.first_known_second_qty,
        s.qty_avg_2_prior_avg_2_prior,
        st.id AS move_id,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at,
        st.unit_price AS move_unit_price
    FROM with_trends s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
SELECT
    move_type,
    sku_id,
    COUNT(*) AS row_count,
    COUNT(DISTINCT move_type) AS distinct_move_types,
    COUNT(DISTINCT id) AS distinct_ids
FROM line_level
GROUP BY CUBE(move_type, sku_id)
