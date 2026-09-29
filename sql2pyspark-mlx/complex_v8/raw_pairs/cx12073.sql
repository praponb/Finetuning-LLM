WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT *
    FROM stock_moves
    WHERE move_type <> 'OUTBOUND'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY warehouse_id, sku_id ORDER BY qty ASC) AS qty_dense_rank,
        SUM(qty) OVER (PARTITION BY warehouse_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        SUM(qty) OVER w_warehouse_id AS warehouse_id_sum_qty,
        DENSE_RANK() OVER (PARTITION BY warehouse_id ORDER BY qty, id) AS qty_dense_rank_2,
        SUM(qty) OVER (PARTITION BY MONTH(moved_at)) AS moved_at_month_sum_qty,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY sku_id, operator ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        MAX(unit_price) OVER (PARTITION BY MONTH(moved_at)) AS moved_at_month_max_unit_price,
        RANK() OVER w_warehouse_id_sku_id_by_qty_t AS qty_rank,
        ROUND(qty - AVG(qty) OVER (PARTITION BY warehouse_id), 2) AS qty_vs_warehouse_id_avg,
        SUM(unit_price) OVER w_warehouse_id AS warehouse_id_sum_unit_price
    FROM base_moves
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id), w_warehouse_id_sku_id_by_qty_t AS (PARTITION BY warehouse_id, sku_id ORDER BY qty ASC)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        qty_dense_rank,
        running_qty,
        warehouse_id_sum_qty,
        qty_dense_rank_2,
        moved_at_month_sum_qty,
        second_unit_price,
        moved_at_month_max_unit_price,
        qty_vs_warehouse_id_avg,
        warehouse_id_sum_unit_price
    FROM ranked
    WHERE qty_rank <= 5
      AND qty_dense_rank < 9
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        move_type,
        COUNT(*) AS cnt,
        SUM(unit_price) AS total_unit_price,
        ROUND(AVG(warehouse_id_sum_unit_price), 2) AS avg_warehouse_id_sum_unit_price,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
    FROM top_per_group
    GROUP BY 1
    HAVING MAX(moved_at_month_sum_qty) > 501
)
-- Final projection
SELECT
    move_type,
    cnt,
    total_unit_price,
    avg_warehouse_id_sum_unit_price,
    operator_list,
    CASE WHEN cnt >= 9 THEN 'HIGH' WHEN cnt >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS cnt_tier
FROM totals
WHERE avg_warehouse_id_sum_unit_price > 1250.8
ORDER BY move_type DESC
