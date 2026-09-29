WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price * 0.25, 2) AS unit_price_adj,
        qty + unit_price AS qty_plus_unit_price,
        LENGTH(TRIM(operator)) AS operator_len
    FROM stock_moves
    WHERE MONTH(moved_at) IN (2, 3)
      AND move_type NOT IN ('OUTBOUND', 'ADJUST')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(warehouse_id) OVER w_sku_id_operator) AS distinct_warehouse_id_in_sku_id_operator,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_sku_id_operator, 0), 4) AS unit_price_share_of_sku_id_operator,
        LAST_VALUE(qty_plus_unit_price) OVER (PARTITION BY operator, operator_len ORDER BY operator_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty_plus_unit_price,
        NTH_VALUE(qty, 2) OVER (PARTITION BY operator_len ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty
    FROM scoped_moves
    WINDOW w_sku_id_operator AS (PARTITION BY sku_id, operator)
),
summary AS (
    -- Summarise per group
    SELECT
        operator,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_ym,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        COUNT(*) AS n_rows,
        ROUND(SUM(unit_price_adj) / NULLIF(SUM(unit_price_share_of_sku_id_operator), 0), 3) AS unit_price_adj_to_unit_price_share_of_sku_id_ope,
        COUNT(DISTINCT id) AS distinct_ids
    FROM sequenced
    GROUP BY operator, DATE_FORMAT(moved_at, 'yyyy-MM')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        operator,
        moved_at_ym,
        n_rows,
        distinct_ids,
        NTH_VALUE(distinct_ids, 2) OVER (ORDER BY distinct_ids, operator, moved_at_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_ids,
        ROUND(distinct_ids - AVG(distinct_ids) OVER (PARTITION BY n_rows), 2) AS distinct_ids_vs_n_rows_avg,
        COUNT(*) OVER (PARTITION BY n_rows) AS rows_in_n_rows,
        ROW_NUMBER() OVER w_all_by_distinct_ids AS seq_no,
        LAG(n_rows, 1, 0) OVER w_distinct_ids_n_rows_by_disti AS prev_n_rows,
        MIN(n_rows) OVER (PARTITION BY n_rows) AS n_rows_min_n_rows
    FROM summary
    WINDOW w_all_by_distinct_ids AS (ORDER BY distinct_ids, operator, moved_at_ym), w_distinct_ids_n_rows_by_disti AS (PARTITION BY distinct_ids, n_rows ORDER BY distinct_ids, operator, moved_at_ym)
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM windowed
    WHERE seq_no <= 2
      AND rows_in_n_rows >= 7
),
annotated AS (
    -- Label and score rows
    SELECT
        operator,
        moved_at_ym,
        second_distinct_ids,
        distinct_ids_vs_n_rows_avg,
        seq_no,
        prev_n_rows,
        LEAST(n_rows, second_distinct_ids) AS least_n_rows_second_distinct_ids,
        UPPER(TRIM(moved_at_ym)) AS moved_at_ym_clean,
        CASE WHEN operator IN ('op_dew', 'op_bee') THEN 'group_a' WHEN operator = 'op_chan' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM leaders
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(seq_no, 3, 0) OVER (ORDER BY distinct_ids_vs_n_rows_avg, operator, moved_at_ym) AS next3_seq_no,
        RANK() OVER w_operator_by_seq_no_ties AS seq_no_rank
    FROM annotated
    WINDOW w_operator_by_seq_no_ties AS (PARTITION BY operator ORDER BY seq_no NULLS LAST)
)
SELECT
    operator,
    moved_at_ym,
    distinct_ids_vs_n_rows_avg,
    seq_no,
    prev_n_rows,
    least_n_rows_second_distinct_ids,
    moved_at_ym_clean,
    next3_seq_no,
    seq_no_rank,
    LEAST(seq_no, next3_seq_no) AS least_seq_no_next3_seq_no,
    ABS(seq_no_rank - 7) AS seq_no_rank_distance
FROM with_windows
WHERE distinct_ids_vs_n_rows_avg >= -7.0
ORDER BY distinct_ids_vs_n_rows_avg DESC
