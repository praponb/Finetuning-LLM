WITH recent_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE warehouse_id <= 5
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(qty) OVER w_operator AS operator_min_qty,
        SUM(qty) OVER w_warehouse_id AS warehouse_id_sum_qty,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY operator ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        MAX(qty) OVER w_warehouse_id AS warehouse_id_max_qty,
        LAST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY warehouse_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        FIRST_VALUE(qty) OVER (PARTITION BY warehouse_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        DENSE_RANK() OVER (PARTITION BY warehouse_id ORDER BY qty DESC) AS qty_dense_rank,
        SUM(qty) OVER (PARTITION BY warehouse_id ORDER BY qty ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS qty_sum_7
    FROM recent_moves
    WINDOW w_operator AS (PARTITION BY operator), w_warehouse_id AS (PARTITION BY warehouse_id)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE qty_dense_rank <= 5
      AND first_qty >= 189
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        qty,
        moved_at,
        operator,
        operator_min_qty,
        second_moved_at,
        last_known_qty,
        first_qty,
        qty_sum_7,
        NTILE(10) OVER w_qty_dense_rank_sku_id_by_qty AS qty_sum_7_decile,
        COUNT(*) OVER (PARTITION BY qty_dense_rank, sku_id ORDER BY qty_sum_7, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY qty_dense_rank, warehouse_id)) AS distinct_operator_in_qty_dense_rank_warehouse_id,
        qty_sum_7 - LAG(qty_sum_7) OVER (PARTITION BY qty_dense_rank, sku_id ORDER BY warehouse_id_sum_qty DESC, id) AS qty_sum_7_change,
        LEAD(first_qty, 2) OVER (PARTITION BY qty_dense_rank, sku_id ORDER BY qty_sum_7, id) AS next2_first_qty
    FROM first_rows
    WINDOW w_qty_dense_rank_sku_id_by_qty AS (PARTITION BY qty_dense_rank, sku_id ORDER BY qty_sum_7, id)
)
SELECT
    id,
    warehouse_id,
    qty,
    moved_at,
    operator_min_qty,
    second_moved_at,
    first_qty,
    qty_sum_7,
    running_count,
    distinct_operator_in_qty_dense_rank_warehouse_id,
    qty_sum_7_change,
    next2_first_qty,
    CASE WHEN operator_min_qty >= 3500 THEN 'A' WHEN operator_min_qty >= 1751 THEN 'B' ELSE 'C' END AS operator_min_qty_tier,
    LENGTH(TRIM(operator)) AS operator_len
FROM with_windows
ORDER BY operator_len
