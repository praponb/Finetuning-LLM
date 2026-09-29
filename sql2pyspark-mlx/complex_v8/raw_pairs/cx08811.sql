WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE operator = 'op_ann'
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        st.id AS move_id,
        st.operator AS move_operator,
        st.qty AS move_qty,
        st.move_type AS move_move_type,
        st.unit_price AS move_unit_price
    FROM prepared_moves s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
move_type_stats AS (
    -- Median unit_price per move_type
    SELECT move_type, PERCENTILE_APPROX(unit_price, 0.5) AS median_unit_price
    FROM detail_rows
    GROUP BY move_type
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_unit_price,
        ROUND(s.unit_price - g.median_unit_price, 2) AS unit_price_minus_median
    FROM detail_rows s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.unit_price >= g.median_unit_price
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        sku_id,
        move_type,
        unit_price,
        move_id,
        move_qty,
        move_move_type,
        move_unit_price,
        unit_price_minus_median,
        COALESCE(unit_price_minus_median, 0.0) AS unit_price_minus_median_filled,
        UPPER(TRIM(move_move_type)) AS move_move_type_clean,
        ABS(qty - 299) AS qty_distance
    FROM compared
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        move_type,
        unit_price,
        move_id,
        move_unit_price,
        unit_price_minus_median,
        unit_price_minus_median_filled,
        move_move_type_clean,
        qty_distance,
        unit_price_minus_median - LAG(unit_price_minus_median) OVER (ORDER BY move_qty NULLS LAST, id, move_id) AS unit_price_minus_median_change,
        NTH_VALUE(unit_price_minus_median, 2) OVER (PARTITION BY move_id ORDER BY unit_price_minus_median_filled ASC NULLS LAST, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_minus_median,
        DENSE_RANK() OVER (PARTITION BY move_id ORDER BY unit_price_minus_median_filled ASC NULLS LAST) AS unit_price_minus_median_filled_dense_rank
    FROM labeled
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.qty < 384
              AND st.warehouse_id IS NOT NULL
        )
      AND s.move_type IS NOT NULL
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count,
        (SELECT SUM(st.unit_price) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS total_move_unit_price
    FROM qualified s
)
SELECT
    sku_id,
    unit_price_minus_median_filled_dense_rank,
    move_type,
    SUM(move_unit_price) AS total_move_unit_price_2
FROM with_counts
GROUP BY ROLLUP(sku_id, unit_price_minus_median_filled_dense_rank, move_type)
ORDER BY sku_id NULLS LAST, unit_price_minus_median_filled_dense_rank NULLS LAST, move_type NULLS LAST
