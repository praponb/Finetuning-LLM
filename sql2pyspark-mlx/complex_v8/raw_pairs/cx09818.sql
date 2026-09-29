WITH recent_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator
    FROM stock_moves
),
with_parent_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        sk.sku_code
    FROM recent_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY warehouse_id ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        FIRST_VALUE(moved_at) OVER (ORDER BY qty NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at,
        NTH_VALUE(qty, 2) OVER (ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        DENSE_RANK() OVER (ORDER BY moved_at ASC, id) AS moved_at_dense_rank,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        qty - LAG(qty) OVER (ORDER BY qty NULLS LAST, id) AS qty_change,
        ROW_NUMBER() OVER (ORDER BY moved_at ASC, id) AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id ORDER BY moved_at ASC), 3) AS moved_at_percent_rank,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_qty,
        ROUND(qty / NULLIF(SUM(qty) OVER w_warehouse_id, 0), 4) AS qty_share_of_warehouse_id,
        RANK() OVER (PARTITION BY move_type ORDER BY qty NULLS LAST, id) AS qty_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY qty NULLS LAST), 3) AS qty_percent_rank,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY warehouse_id)) AS distinct_move_type_in_warehouse_id,
        SUM(qty) OVER (PARTITION BY sku_id) AS sku_id_sum_qty
    FROM with_parent_sku
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id)
),
scored AS (
    -- Add calculated columns
    SELECT *, COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM with_trends
    WHERE moved_at >= TIMESTAMP '2025-02-06 08:00:00'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        first_moved_at,
        second_qty,
        moved_at_dense_rank,
        rows_in_sku_id,
        rn,
        sku_id_avg_qty,
        qty_share_of_warehouse_id,
        qty_rank,
        qty_percent_rank,
        distinct_move_type_in_warehouse_id,
        sku_id_sum_qty,
        move_type_or_unknown,
        LEAD(rows_in_sku_id, 3) OVER w_move_type_or_unknown_by_qty AS next3_rows_in_sku_id,
        second_qty - LAG(second_qty) OVER (ORDER BY qty_change, id) AS second_qty_change,
        NTH_VALUE(qty_change, 2) OVER (PARTITION BY move_type_or_unknown ORDER BY qty_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty_change_2,
        ROUND(CUME_DIST() OVER (ORDER BY qty_change), 3) AS qty_change_cume_dist,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY DATE_TRUNC('MONTH', second_moved_at))) AS distinct_warehouse_id_in_second_moved_at_month,
        COUNT(*) OVER w_qty_rank AS rows_in_qty_rank,
        SUM(qty_share_of_warehouse_id) OVER (ORDER BY qty_change, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty_share_of_warehouse_id
    FROM scored
    WINDOW w_move_type_or_unknown_by_qty AS (PARTITION BY move_type_or_unknown ORDER BY qty_rank DESC, id), w_qty_rank AS (PARTITION BY qty_rank)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    first_moved_at,
    second_qty,
    moved_at_dense_rank,
    rows_in_sku_id,
    rn,
    sku_id_avg_qty,
    qty_share_of_warehouse_id,
    qty_rank,
    qty_percent_rank,
    sku_id_sum_qty,
    move_type_or_unknown,
    next3_rows_in_sku_id,
    second_qty_change,
    second_qty_change_2,
    running_qty_share_of_warehouse_id,
    CASE WHEN qty_percent_rank >= 0.8 THEN 'A' WHEN qty_percent_rank >= 0.4 THEN 'B' ELSE 'C' END AS qty_percent_rank_tier
FROM sequenced
