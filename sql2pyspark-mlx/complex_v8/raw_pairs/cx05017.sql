WITH scoped AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-12'
),
combined_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.move_type_slug,
        w.opened_on AS warehouse_opened_on
    FROM scoped s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        moved_at,
        operator,
        move_type_slug,
        warehouse_opened_on,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id, operator ORDER BY moved_at DESC), 3) AS moved_at_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(warehouse_opened_on) ORDER BY qty), 3) AS qty_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id, operator ORDER BY moved_at, id) AS rn,
        ROUND(AVG(qty) OVER (PARTITION BY YEAR(warehouse_opened_on) ORDER BY qty, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_6_prior,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id, DATE_TRUNC('MONTH', moved_at))) AS distinct_sku_id_in_warehouse_id_moved_at_month,
        NTILE(3) OVER (PARTITION BY YEAR(warehouse_opened_on) ORDER BY qty, id) AS qty_tercile,
        LAG(warehouse_opened_on) OVER (PARTITION BY YEAR(warehouse_opened_on) ORDER BY qty, id) AS prev_warehouse_opened_on,
        ROUND(qty - AVG(qty) OVER (PARTITION BY warehouse_id, DATE_TRUNC('MONTH', moved_at)), 2) AS qty_vs_warehouse_id_moved_at_month_avg,
        SUM(qty) OVER (PARTITION BY warehouse_id, operator ORDER BY moved_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_sum_2_prior,
        MIN(qty) OVER (PARTITION BY warehouse_id, DATE_TRUNC('MONTH', moved_at)) AS warehouse_id_moved_at_month_min_qty,
        RANK() OVER (PARTITION BY sku_id, operator ORDER BY moved_at DESC, id) AS moved_at_rank,
        COUNT(*) OVER (PARTITION BY warehouse_id, operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROW_NUMBER() OVER (PARTITION BY sku_id, operator ORDER BY moved_at DESC, id) AS moved_at_seq,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY warehouse_id, DATE_TRUNC('MONTH', moved_at)), 0), 4) AS qty_share_of_warehouse_id_moved_at_month,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id, operator ORDER BY moved_at), 3) AS moved_at_cume_dist_2
    FROM combined_details_warehouse
    WINDOW w_warehouse_id_moved_at_month AS (PARTITION BY warehouse_id, DATE_TRUNC('MONTH', moved_at))
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(move_type_slug), 1, 4) AS move_type_slug_prefix,
        CONCAT(move_type_slug, '#', CAST(qty_sum_2_prior AS STRING)) AS move_type_slug_ref,
        ROUND(qty_tercile / NULLIF(moved_at_cume_dist, 0), 3) AS qty_tercile_per_moved_at_cume_dist
    FROM metrics
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        moved_at,
        operator,
        move_type_slug,
        distinct_sku_id_in_warehouse_id_moved_at_month,
        qty_tercile,
        prev_warehouse_opened_on,
        qty_vs_warehouse_id_moved_at_month_avg,
        qty_sum_2_prior,
        warehouse_id_moved_at_month_min_qty,
        moved_at_rank,
        running_count,
        moved_at_cume_dist_2,
        move_type_slug_prefix,
        qty_tercile_per_moved_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on), distinct_sku_id_in_warehouse_id_moved_at_month ORDER BY moved_at_seq), 3) AS moved_at_seq_percent_rank,
        ROUND(AVG(running_count) OVER (PARTITION BY operator), 2) AS operator_avg_running_count,
        LEAD(moved_at_seq) OVER (ORDER BY moved_at_rank ASC NULLS LAST, id) AS next_moved_at_seq,
        NTH_VALUE(moved_at_rank, 2) OVER (ORDER BY moved_at_rank ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at_rank,
        MAX(rn) OVER (ORDER BY moved_at_rank ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rn_max_2,
        NTH_VALUE(prev_warehouse_opened_on, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on), distinct_sku_id_in_warehouse_id_moved_at_month ORDER BY moved_at_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev_warehouse_opened_on,
        NTH_VALUE(rn, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on), distinct_sku_id_in_warehouse_id_moved_at_month ORDER BY moved_at_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rn
    FROM annotated
)
SELECT
    id,
    moved_at,
    operator,
    move_type_slug,
    qty_vs_warehouse_id_moved_at_month_avg,
    qty_sum_2_prior,
    moved_at_rank,
    moved_at_cume_dist_2,
    move_type_slug_prefix,
    qty_tercile_per_moved_at_cume_dist,
    operator_avg_running_count,
    next_moved_at_seq,
    second_moved_at_rank,
    rn_max_2,
    second_prev_warehouse_opened_on,
    second_rn,
    ROUND(second_rn / NULLIF(moved_at_cume_dist_2, 0), 3) AS second_rn_per_moved_at_cume_dist_2
FROM with_windows
