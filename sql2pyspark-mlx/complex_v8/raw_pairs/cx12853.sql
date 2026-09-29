WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        CASE WHEN operator IN ('op_eve', 'op_ann') THEN 'group_a' WHEN operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY operator_group ORDER BY moved_at NULLS LAST), 3) AS moved_at_cume_dist,
        LAST_VALUE(unit_price) OVER (PARTITION BY sku_id, warehouse_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
        unit_price - LAG(unit_price) OVER w_sku_id_warehouse_id_by_qty AS unit_price_change,
        SUM(unit_price) OVER (PARTITION BY operator_group ORDER BY moved_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        COUNT(*) OVER w_warehouse_id AS rows_in_warehouse_id,
        SIZE(COLLECT_SET(operator_group) OVER (PARTITION BY warehouse_id)) AS distinct_operator_group_in_warehouse_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator_group ORDER BY moved_at NULLS LAST, id), 3) AS moved_at_percent_rank,
        SUM(qty) OVER (PARTITION BY warehouse_id, operator_group ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id, warehouse_id ORDER BY qty), 3) AS qty_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator_group ORDER BY moved_at NULLS LAST), 3) AS moved_at_percent_rank_2,
        SUM(unit_price) OVER (PARTITION BY sku_id, warehouse_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price_2,
        qty - LAG(qty) OVER w_warehouse_id_operator_group_2 AS qty_change
    FROM filtered
    WINDOW w_sku_id_warehouse_id_by_qty AS (PARTITION BY sku_id, warehouse_id ORDER BY qty, id), w_warehouse_id AS (PARTITION BY warehouse_id), w_warehouse_id_operator_group_2 AS (PARTITION BY warehouse_id, operator_group ORDER BY qty ASC, id)
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_all_by_moved_at AS moved_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY operator_group, warehouse_id ORDER BY unit_price), 3) AS unit_price_cume_dist,
        LAST_VALUE(moved_at) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        COUNT(*) OVER w_sku_id AS rows_in_sku_id,
        NTH_VALUE(running_unit_price_2, 2) OVER (PARTITION BY warehouse_id, operator_group ORDER BY running_unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_running_unit_price_2,
        SUM(running_unit_price) OVER (PARTITION BY warehouse_id, operator_group ORDER BY running_unit_price ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running_unit_price_sum_3
    FROM sequenced
    WINDOW w_all_by_moved_at AS (ORDER BY moved_at DESC, id), w_sku_id AS (PARTITION BY sku_id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    unit_price,
    operator_group,
    last_unit_price,
    unit_price_change,
    rows_in_warehouse_id,
    distinct_operator_group_in_warehouse_id,
    moved_at_percent_rank,
    moved_at_percent_rank_2,
    qty_change,
    rows_in_sku_id,
    second_running_unit_price_2
FROM sequenced_2
ORDER BY rows_in_warehouse_id, moved_at_percent_rank_2 DESC
