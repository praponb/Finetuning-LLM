WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE warehouse_id <= 3
      AND operator <> 'op_dew'
),
with_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        sk.sku_code
    FROM valid_moves s
    JOIN skus sk ON sk.id = s.sku_id AND sk.sku_code IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        moved_at,
        sku_code,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at), 3) AS moved_at_cume_dist,
        DENSE_RANK() OVER (ORDER BY moved_at DESC) AS moved_at_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id ORDER BY moved_at), 3) AS moved_at_cume_dist_2,
        ROUND(CUME_DIST() OVER (ORDER BY moved_at DESC), 3) AS moved_at_cume_dist_3,
        NTH_VALUE(moved_at, 2) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        NTILE(5) OVER (ORDER BY moved_at DESC, id) AS moved_at_quintile,
        NTILE(10) OVER (ORDER BY moved_at DESC, id) AS moved_at_decile,
        DENSE_RANK() OVER w_all_by_moved_at_ties_2 AS moved_at_dense_rank_2,
        NTILE(3) OVER (ORDER BY moved_at DESC, id) AS moved_at_tercile
    FROM with_details_sku
    WINDOW w_all_by_moved_at_ties_2 AS (ORDER BY moved_at ASC)
)
SELECT
    id,
    warehouse_id,
    moved_at,
    moved_at_cume_dist,
    moved_at_dense_rank,
    moved_at_cume_dist_2,
    moved_at_cume_dist_3,
    second_moved_at,
    moved_at_quintile,
    moved_at_decile,
    LOWER(TRIM(sku_code)) AS sku_code_clean,
    ROUND(moved_at_cume_dist_3 * 1.07, 2) AS moved_at_cume_dist_3_adj
FROM ranked
ORDER BY sku_code_clean NULLS LAST, moved_at_quintile DESC NULLS FIRST
