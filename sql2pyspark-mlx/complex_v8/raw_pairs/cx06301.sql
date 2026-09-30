WITH scoped_moves AS (
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
        FLOOR(qty / 100) * 100 AS qty_bucket,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
    WHERE unit_price BETWEEN 37.6 AND 313.8
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id, move_type), 2) AS unit_price_vs_sku_id_move_type_avg,
        MAX(qty) OVER (PARTITION BY warehouse_id ORDER BY qty ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS qty_max_1_prior,
        SIZE(COLLECT_SET(operator_or_unknown) OVER (PARTITION BY sku_id, move_type)) AS distinct_operator_or_unknown_in_sku_id_move_type,
        RANK() OVER w_warehouse_id_by_qty_ties AS qty_rank
    FROM scoped_moves
    WINDOW w_warehouse_id_by_qty_ties AS (PARTITION BY warehouse_id ORDER BY qty ASC)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(qty_max_1_prior, 2) OVER (ORDER BY qty_rank, id) AS next2_qty_max_1_prior,
        SUM(qty_rank) OVER (ORDER BY qty_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS qty_rank_sum_3_prior,
        NTILE(4) OVER (ORDER BY qty_rank, id) AS qty_rank_quartile,
        MIN(qty_rank) OVER w_operator_or_unknown_distinct AS operator_or_unknown_distinct_operator_or_unknown
    FROM with_windows
    WINDOW w_operator_or_unknown_distinct AS (PARTITION BY operator_or_unknown, distinct_operator_or_unknown_in_sku_id_move_type)
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM with_trends s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.operator IN ('op_dew', 'op_eve', 'op_ann')
              AND st.qty >= 98
        )
      AND s.warehouse_id IS NOT NULL
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM without_matches s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.unit_price >= 150.7
              AND st.move_type NOT IN ('ADJUST', 'OUTBOUND')
        )
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM filtered
    WHERE qty_rank <= 5
)
SELECT *
FROM (
    SELECT qty_rank_quartile, operator, operator_or_unknown_distinct_operator_or_unknown
    FROM first_rows
) src
PIVOT (
    AVG(operator_or_unknown_distinct_operator_or_unknown)
    FOR operator IN ('op_eve', 'op_chan', 'op_bee')
)
ORDER BY qty_rank_quartile
