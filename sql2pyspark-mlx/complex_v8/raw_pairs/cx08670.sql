WITH recent_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        ROUND(unit_price * 1.07, 2) AS unit_price_adj,
        DATE_TRUNC('QUARTER', moved_at) AS moved_at_quarter
    FROM stock_moves
    WHERE unit_price < 299.1
      AND qty >= 116
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id, move_type ORDER BY moved_at NULLS LAST, id), 3) AS moved_at_percent_rank,
        SUM(unit_price_adj) OVER (PARTITION BY sku_id ORDER BY qty, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS unit_price_adj_sum_2,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id, move_type ORDER BY moved_at NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY qty, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY qty) AS qty_dense_rank,
        NTILE(3) OVER (PARTITION BY sku_id, move_type ORDER BY moved_at NULLS LAST, id) AS moved_at_tercile
    FROM recent_moves
),
unique_rows AS (
    -- Keep the latest row per sku_id, warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price_filled,
        unit_price_adj,
        moved_at_quarter,
        moved_at_percent_rank,
        unit_price_adj_sum_2,
        qty_avg_3_prior,
        seq_no,
        qty_dense_rank,
        moved_at_tercile
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, warehouse_id ORDER BY qty_dense_rank DESC, id DESC) AS recency_rank
        FROM with_windows
    ) ranked
    WHERE recency_rank = 1
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.moved_at_percent_rank,
        s.qty_avg_3_prior,
        s.qty_dense_rank,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.moved_at AS move_moved_at,
        st.qty AS move_qty,
        st.operator AS move_operator
    FROM unique_rows s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM line_level s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.unit_price < 273.5
        )
)
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    qty_dense_rank,
    move_moved_at,
    move_qty,
    LENGTH(TRIM(move_move_type)) AS move_move_type_len,
    COALESCE(move_operator, 'UNKNOWN') AS move_operator_or_unknown
FROM filtered
WHERE move_qty >= 67
ORDER BY moved_at DESC
