WITH valid AS (
    -- Keep valid rows and derive helper columns
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
    WHERE operator IN ('op_ann', 'op_bee')
      AND move_type = 'TRANSFER'
),
combined_details_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.sku_code
    FROM valid s
    JOIN skus sk ON sk.id = s.sku_id AND sk.category IS NOT NULL
    WHERE LENGTH(TRIM(sk.sku_code)) >= 6
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        qty,
        operator,
        sku_code,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        ROUND(PERCENT_RANK() OVER w_operator_by_moved_at, 3) AS moved_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY operator ORDER BY moved_at, id), 3) AS moved_at_cume_dist,
        DENSE_RANK() OVER w_all_by_unit_price_ties AS unit_price_dense_rank,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type), 2) AS move_type_avg_unit_price,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator, MONTH(moved_at) ORDER BY unit_price NULLS LAST, id), 3) AS unit_price_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY unit_price DESC), 3) AS unit_price_cume_dist,
        LAG(moved_at) OVER (PARTITION BY operator, MONTH(moved_at) ORDER BY unit_price NULLS LAST, id) AS prev_moved_at,
        ROUND(PERCENT_RANK() OVER w_all_by_unit_price_ties, 3) AS unit_price_percent_rank_2,
        ROUND(CUME_DIST() OVER (PARTITION BY operator, MONTH(moved_at) ORDER BY unit_price NULLS LAST), 3) AS unit_price_cume_dist_2
    FROM combined_details_sku
    WINDOW w_operator_by_moved_at AS (PARTITION BY operator ORDER BY moved_at, id), w_all_by_unit_price_ties AS (ORDER BY unit_price DESC)
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(prev_moved_at, 'yyyy-MM') AS prev_moved_at_ym,
        unit_price_dense_rank,
        operator,
        ROUND(SUM(moved_at_cume_dist) / NULLIF(SUM(qty), 0), 3) AS moved_at_cume_dist_to_qty_ratio,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(sku_code)) AS sku_code_list
    FROM sequenced
    GROUP BY DATE_FORMAT(prev_moved_at, 'yyyy-MM'), unit_price_dense_rank, operator
    HAVING COUNT(*) >= 2
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        prev_moved_at_ym,
        unit_price_dense_rank,
        operator,
        moved_at_cume_dist_to_qty_ratio,
        row_count,
        sku_code_list,
        LEAD(unit_price_dense_rank) OVER (PARTITION BY row_count, operator ORDER BY row_count NULLS LAST, prev_moved_at_ym, unit_price_dense_rank, operator) AS next_unit_price_dense_rank,
        ROUND(unit_price_dense_rank - AVG(unit_price_dense_rank) OVER (PARTITION BY operator), 2) AS unit_price_dense_rank_vs_operator_avg,
        LAST_VALUE(unit_price_dense_rank) IGNORE NULLS OVER (PARTITION BY row_count, operator ORDER BY row_count NULLS LAST, prev_moved_at_ym, unit_price_dense_rank, operator ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price_dense_rank,
        LAG(unit_price_dense_rank) OVER (PARTITION BY row_count, operator ORDER BY row_count NULLS LAST, prev_moved_at_ym, unit_price_dense_rank, operator) AS prev_unit_price_dense_rank
    FROM per_group
)
SELECT
    prev_moved_at_ym,
    operator,
    moved_at_cume_dist_to_qty_ratio,
    row_count,
    sku_code_list,
    next_unit_price_dense_rank,
    last_known_unit_price_dense_rank
FROM sequenced_2
WHERE prev_moved_at_ym IS NOT NULL
ORDER BY next_unit_price_dense_rank
