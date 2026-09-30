WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE warehouse_id <= 5
      AND move_type NOT IN ('ADJUST', 'OUTBOUND')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id ORDER BY moved_at NULLS LAST), 3) AS moved_at_cume_dist,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY YEAR(moved_at)), 2) AS unit_price_vs_moved_at_year_avg,
        RANK() OVER (ORDER BY unit_price DESC) AS unit_price_rank,
        ROUND(qty - AVG(qty) OVER (PARTITION BY warehouse_id), 2) AS qty_vs_warehouse_id_avg,
        MAX(unit_price) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS unit_price_max_1_prior,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_id), 2) AS warehouse_id_avg_qty,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id ORDER BY moved_at), 3) AS moved_at_cume_dist_2,
        LEAD(qty, 2, 0) OVER w_sku_id_by_moved_at AS next2_qty,
        NTILE(4) OVER (PARTITION BY move_type ORDER BY qty NULLS LAST, id) AS qty_quartile,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY warehouse_id ORDER BY moved_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        FIRST_VALUE(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at,
        DENSE_RANK() OVER (PARTITION BY warehouse_id ORDER BY moved_at NULLS LAST) AS moved_at_dense_rank,
        NTILE(3) OVER (PARTITION BY warehouse_id ORDER BY moved_at NULLS LAST, id) AS moved_at_tercile,
        SUM(unit_price) OVER (PARTITION BY YEAR(moved_at)) AS moved_at_year_sum_unit_price
    FROM filtered_moves
    WINDOW w_sku_id_by_moved_at AS (PARTITION BY sku_id ORDER BY moved_at, id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type)) AS distinct_sku_id_in_move_type,
        COUNT(*) OVER w_moved_at_tercile AS rows_in_moved_at_tercile,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id, moved_at_tercile ORDER BY first_moved_at, id), 3) AS first_moved_at_cume_dist,
        ROUND(moved_at_year_sum_unit_price - AVG(moved_at_year_sum_unit_price) OVER (PARTITION BY moved_at_tercile), 2) AS moved_at_year_sum_unit_price_vs_moved_at_tercile,
        ROW_NUMBER() OVER w_sku_id_moved_at_tercile_by_f AS rn
    FROM windowed
    WINDOW w_moved_at_tercile AS (PARTITION BY moved_at_tercile), w_sku_id_moved_at_tercile_by_f AS (PARTITION BY sku_id, moved_at_tercile ORDER BY first_moved_at, id)
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    unit_price,
    moved_at_cume_dist,
    unit_price_vs_moved_at_year_avg,
    unit_price_rank,
    qty_vs_warehouse_id_avg,
    unit_price_max_1_prior,
    moved_at_cume_dist_2,
    next2_qty,
    qty_quartile,
    second_moved_at,
    moved_at_tercile,
    moved_at_year_sum_unit_price,
    distinct_sku_id_in_move_type,
    first_moved_at_cume_dist,
    moved_at_year_sum_unit_price_vs_moved_at_tercile,
    rn,
    LENGTH(TRIM(move_type)) AS move_type_len,
    SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix
FROM with_trends
ORDER BY qty_quartile DESC NULLS FIRST, rn
