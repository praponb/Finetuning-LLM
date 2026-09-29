WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        LENGTH(TRIM(move_type)) AS move_type_len,
        CASE WHEN qty >= 372 THEN 'large' WHEN qty >= 116 THEN 'medium' ELSE 'small' END AS qty_tier,
        SUBSTRING(TRIM(move_type), 1, 4) AS move_type_prefix
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND unit_price >= 136.0
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM prepared_moves s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.operator <> 'op_bee'
              AND st.qty BETWEEN 145 AND 271
        )
      AND s.move_type_prefix IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(unit_price) OVER (PARTITION BY qty_tier, operator ORDER BY move_type_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        ROUND(AVG(move_type_len) OVER (PARTITION BY qty_tier, operator ORDER BY move_type_len, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS move_type_len_avg_6_prior,
        SUM(move_type_len) OVER w_move_type_len AS move_type_len_sum_move_type_len,
        COUNT(*) OVER (PARTITION BY move_type_len) AS rows_in_move_type_len
    FROM without_matches
    WINDOW w_move_type_len AS (PARTITION BY move_type_len)
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count_2
    FROM metrics s
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM summarized_rows s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
        )
      AND s.move_count_2 <= 7
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        move_type,
        qty,
        moved_at,
        move_type_len,
        move_type_prefix,
        move_type_len_avg_6_prior,
        move_type_len_sum_move_type_len,
        move_count,
        move_count_2,
        ROUND(CUME_DIST() OVER (ORDER BY qty, id), 3) AS qty_cume_dist,
        COUNT(*) OVER (PARTITION BY operator) AS rows_in_operator,
        ROUND(AVG(rows_in_move_type_len) OVER (ORDER BY move_count_2, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS rows_in_move_type_len_avg_1_prior
    FROM qualified
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, metric_value
FROM (
    SELECT id, move_count, move_count_2, move_type_len
    FROM with_trends
) src
UNPIVOT (
    metric_value FOR metric IN (move_count, move_count_2, move_type_len)
) unp
ORDER BY id, metric
