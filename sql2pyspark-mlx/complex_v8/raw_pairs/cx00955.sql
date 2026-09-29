WITH recent_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        ROUND(qty - AVG(qty) OVER (PARTITION BY warehouse_id), 2) AS qty_vs_warehouse_id_avg,
        MAX(qty) OVER (PARTITION BY warehouse_id) AS warehouse_id_max_qty,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id)) AS distinct_sku_id_in_warehouse_id,
        SUM(qty) OVER (ORDER BY qty DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS qty_sum_7,
        NTH_VALUE(moved_at, 2) OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        SUM(unit_price) OVER (PARTITION BY warehouse_id) AS warehouse_id_sum_unit_price,
        LAST_VALUE(qty) OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        SUM(qty) OVER (PARTITION BY warehouse_id) AS warehouse_id_sum_qty,
        ROW_NUMBER() OVER (ORDER BY qty DESC, id) AS seq_no,
        NTH_VALUE(qty, 2) OVER (PARTITION BY sku_id, operator ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty
    FROM recent_moves
),
windowed_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        unit_price,
        warehouse_id_max_qty,
        distinct_sku_id_in_warehouse_id,
        qty_sum_7,
        warehouse_id_sum_unit_price,
        seq_no,
        second_qty,
        LAST_VALUE(qty_sum_7) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY second_moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty_sum_7,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY second_moved_at ASC), 3) AS second_moved_at_percent_rank,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY distinct_sku_id_in_warehouse_id, warehouse_id)) AS distinct_sku_id_in_distinct_sku_id_in_warehouse_,
        ROUND(warehouse_id_max_qty / NULLIF(SUM(warehouse_id_max_qty) OVER (PARTITION BY distinct_sku_id_in_warehouse_id, warehouse_id), 0), 4) AS warehouse_id_max_qty_share_of_distinct_sku_id_in,
        NTH_VALUE(warehouse_id_max_qty, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY second_moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_id_max_qty
    FROM windowed
    WINDOW w_distinct_sku_id_in_warehouse AS (PARTITION BY distinct_sku_id_in_warehouse_id, warehouse_id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    unit_price,
    warehouse_id_max_qty,
    distinct_sku_id_in_warehouse_id,
    qty_sum_7,
    seq_no,
    last_qty_sum_7,
    second_moved_at_percent_rank,
    distinct_sku_id_in_distinct_sku_id_in_warehouse_,
    warehouse_id_max_qty_share_of_distinct_sku_id_in,
    second_warehouse_id_max_qty,
    ABS(qty_sum_7 - 676) AS qty_sum_7_distance,
    qty_sum_7 + seq_no AS qty_sum_7_plus_seq_no
FROM windowed_2
ORDER BY distinct_sku_id_in_warehouse_id NULLS LAST, sku_id
