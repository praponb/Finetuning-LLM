WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE unit_price >= 61.2
      AND warehouse_id IS NOT NULL
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        sku_id,
        qty,
        operator,
        operator_move_type_label,
        LENGTH(TRIM(move_type)) AS move_type_len,
        LOWER(TRIM(operator_move_type_label)) AS operator_move_type_label_clean
    FROM scoped
    WHERE move_type <> 'ADJUST'
),
operator_stats AS (
    -- Avg move_type_len per operator
    SELECT operator, AVG(move_type_len) AS avg_move_type_len
    FROM derived
    GROUP BY operator
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_move_type_len,
        ROUND(s.move_type_len - g.avg_move_type_len, 2) AS move_type_len_minus_avg
    FROM derived s
    JOIN operator_stats g ON g.operator = s.operator
    WHERE s.move_type_len > g.avg_move_type_len
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        operator,
        operator_move_type_label,
        move_type_len,
        operator_move_type_label_clean,
        avg_move_type_len,
        move_type_len_minus_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_type_len, operator ORDER BY qty ASC, id), 3) AS qty_percent_rank,
        SIZE(COLLECT_SET(operator) OVER w_sku_id) AS distinct_operator_in_sku_id,
        COUNT(*) OVER w_sku_id AS rows_in_sku_id,
        COUNT(*) OVER (PARTITION BY move_type_len, operator ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM vs_group
    WINDOW w_sku_id AS (PARTITION BY sku_id)
),
latest AS (
    -- Keep the latest row per operator
    SELECT
        id,
        operator,
        operator_move_type_label,
        move_type_len,
        operator_move_type_label_clean,
        avg_move_type_len,
        move_type_len_minus_avg,
        qty_percent_rank,
        distinct_operator_in_sku_id,
        rows_in_sku_id,
        running_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator ORDER BY qty_percent_rank DESC, id DESC) AS rn
        FROM ranked
    ) ranked
    WHERE rn = 1
),
derived_2 AS (
    -- Derive helper columns
    SELECT
        id,
        operator_move_type_label,
        move_type_len,
        avg_move_type_len,
        move_type_len_minus_avg,
        qty_percent_rank,
        distinct_operator_in_sku_id,
        UPPER(TRIM(operator_move_type_label)) AS operator_move_type_label_clean_2,
        COALESCE(running_count, 0) AS running_count_filled
    FROM latest
)
SELECT id, field, amount
FROM (
    SELECT id, move_type_len_minus_avg, qty_percent_rank
    FROM derived_2
) src
UNPIVOT (
    amount FOR field IN (move_type_len_minus_avg, qty_percent_rank)
) unp
