WITH prepared_moves AS (
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
        LENGTH(TRIM(operator)) AS operator_len
    FROM stock_moves
    WHERE (unit_price <= 367.7 OR qty >= 91)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(unit_price) OVER (PARTITION BY operator, sku_id), 2) AS operator_sku_id_avg_unit_price,
        SUM(unit_price) OVER (PARTITION BY operator_len ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        RANK() OVER (ORDER BY operator_len ASC) AS operator_len_rank,
        ROUND(PERCENT_RANK() OVER w_sku_id_by_qty_ties, 3) AS qty_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY qty DESC, id) AS seq_no,
        LAG(qty) OVER (PARTITION BY sku_id ORDER BY qty DESC, id) AS prev_qty,
        MIN(operator_len) OVER (PARTITION BY sku_id ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_operator_len_so_far,
        LAST_VALUE(operator_len) OVER (PARTITION BY sku_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_operator_len,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY operator, sku_id)) AS distinct_warehouse_id_in_operator_sku_id,
        MIN(qty) OVER (PARTITION BY operator, sku_id) AS operator_sku_id_min_qty,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER w_sku_id_by_qty_2)) / 60, 1) AS minutes_since_prev_moved_at,
        ROUND(qty / NULLIF(SUM(qty) OVER w_operator_sku_id, 0), 4) AS qty_share_of_operator_sku_id
    FROM prepared_moves
    WINDOW w_sku_id_by_qty_ties AS (PARTITION BY sku_id ORDER BY qty DESC), w_sku_id_by_qty_2 AS (PARTITION BY sku_id ORDER BY qty, id), w_operator_sku_id AS (PARTITION BY operator, sku_id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        operator_len,
        operator_sku_id_avg_unit_price,
        running_unit_price,
        qty_percent_rank,
        seq_no,
        prev_qty,
        min_operator_len_so_far,
        last_operator_len,
        distinct_warehouse_id_in_operator_sku_id,
        operator_sku_id_min_qty,
        minutes_since_prev_moved_at,
        qty_share_of_operator_sku_id
    FROM ranked
    WHERE operator_len_rank = 1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        operator_len,
        running_unit_price,
        qty_percent_rank,
        seq_no,
        prev_qty,
        min_operator_len_so_far,
        last_operator_len,
        operator_sku_id_min_qty,
        minutes_since_prev_moved_at,
        qty_share_of_operator_sku_id,
        SUM(operator_len) OVER w_seq_no_warehouse_id AS seq_no_warehouse_id_sum_operator_len,
        MAX(prev_qty) OVER (PARTITION BY sku_id ORDER BY seq_no, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS prev_qty_max_3,
        ROW_NUMBER() OVER w_sku_id_by_seq_no AS seq_no_2,
        ROW_NUMBER() OVER w_all_by_operator_len AS operator_len_seq,
        SUM(operator_len) OVER (PARTITION BY move_type, distinct_warehouse_id_in_operator_sku_id) AS move_type_distinct_warehouse_id_in_operator_sku_,
        COUNT(*) OVER w_seq_no_warehouse_id AS rows_in_seq_no_warehouse_id
    FROM top_rows
    WINDOW w_seq_no_warehouse_id AS (PARTITION BY seq_no, warehouse_id), w_sku_id_by_seq_no AS (PARTITION BY sku_id ORDER BY seq_no, id), w_all_by_operator_len AS (ORDER BY operator_len ASC NULLS LAST, id)
)
SELECT
    id,
    operator,
    operator_len,
    running_unit_price,
    qty_percent_rank,
    seq_no,
    prev_qty,
    min_operator_len_so_far,
    operator_sku_id_min_qty,
    minutes_since_prev_moved_at,
    qty_share_of_operator_sku_id,
    prev_qty_max_3,
    seq_no_2,
    operator_len_seq,
    move_type_distinct_warehouse_id_in_operator_sku_,
    rows_in_seq_no_warehouse_id,
    COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
    SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix
FROM metrics
ORDER BY operator_or_unknown
