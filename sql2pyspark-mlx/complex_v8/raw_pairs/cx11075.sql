WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT(move_type, '#', CAST(sku_id AS STRING)) AS move_type_ref,
        CASE WHEN move_type IN ('INBOUND', 'ADJUST') THEN 'group_a' WHEN move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-24'
      AND sku_id IS NOT NULL
),
joined_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_ref,
        s.move_type_group,
        sk.unit_cost AS sku_unit_cost
    FROM scoped_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY warehouse_id ORDER BY qty, id) AS qty_dense_rank,
        SUM(qty) OVER (PARTITION BY move_type_group ORDER BY moved_at NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS qty_sum_3_prior,
        NTH_VALUE(sku_unit_cost, 2) OVER (PARTITION BY warehouse_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_unit_cost,
        NTH_VALUE(unit_price, 2) OVER (ORDER BY moved_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        NTILE(4) OVER (ORDER BY moved_at ASC NULLS LAST, id) AS moved_at_quartile,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY warehouse_id), 0), 4) AS qty_share_of_warehouse_id,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id), 2) AS warehouse_id_avg_unit_price,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id)) AS distinct_sku_id_in_warehouse_id,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY sku_id, move_type_group ORDER BY moved_at ASC, id))) / 60, 1) AS minutes_since_prev_moved_at,
        MAX(qty) OVER (ORDER BY moved_at ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_max_2_prior,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id), 2) AS unit_price_vs_warehouse_id_avg,
        ROW_NUMBER() OVER w_warehouse_id_by_qty AS rn,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY sku_id, move_type_group ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_2,
        ROUND(sku_unit_cost / NULLIF(SUM(sku_unit_cost) OVER (PARTITION BY warehouse_id), 0), 4) AS sku_unit_cost_share_of_warehouse_id
    FROM joined_sku
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id), w_warehouse_id_by_qty AS (PARTITION BY warehouse_id ORDER BY qty, id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(operator)) AS operator_len,
        CONCAT_WS(' / ', move_type_group, move_type_ref) AS move_type_group_move_type_ref_label
    FROM with_windows
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(qty_max_2_prior) OVER (PARTITION BY warehouse_id ORDER BY qty_sum_3_prior, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS qty_max_2_prior_avg_3_prior,
        NTH_VALUE(operator_len, 2) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_operator_len,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY warehouse_id ORDER BY qty_sum_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_3,
        SIZE(COLLECT_SET(move_type_group) OVER (PARTITION BY distinct_sku_id_in_warehouse_id, rn)) AS distinct_move_type_group_in_distinct_sku_id_in_w,
        LEAD(warehouse_id_avg_unit_price, 2) OVER w_warehouse_id_by_qty_sum_3_pr_3 AS next2_warehouse_id_avg_unit_price
    FROM enriched
    WINDOW w_warehouse_id_by_qty_sum_3_pr_3 AS (PARTITION BY warehouse_id ORDER BY qty_sum_3_prior, id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    operator,
    unit_price,
    move_type_ref,
    move_type_group,
    sku_unit_cost,
    qty_dense_rank,
    qty_sum_3_prior,
    second_unit_price,
    moved_at_quartile,
    qty_share_of_warehouse_id,
    warehouse_id_avg_unit_price,
    distinct_sku_id_in_warehouse_id,
    qty_max_2_prior,
    sku_unit_cost_share_of_warehouse_id,
    operator_len,
    move_type_group_move_type_ref_label,
    qty_max_2_prior_avg_3_prior,
    second_operator_len,
    second_unit_price_3,
    LENGTH(TRIM(move_type_group)) AS move_type_group_len
FROM sequenced
ORDER BY moved_at_quartile NULLS LAST, moved_at DESC NULLS FIRST
