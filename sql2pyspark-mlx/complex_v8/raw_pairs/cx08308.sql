WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        CASE WHEN qty >= 393 THEN 'A' WHEN qty >= 191 THEN 'B' ELSE 'C' END AS qty_tier
    FROM stock_moves
    WHERE MONTH(moved_at) IN (2, 3)
      AND warehouse_id <= 3
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(unit_price) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS unit_price_max_3_prior,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type, qty_tier), 2) AS move_type_qty_tier_avg_unit_price,
        ROW_NUMBER() OVER (ORDER BY moved_at DESC, id) AS moved_at_seq,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY move_type, qty_tier), 2) AS unit_price_vs_move_type_qty_tier_avg,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        COUNT(*) OVER (PARTITION BY move_type, qty_tier) AS rows_in_move_type_qty_tier,
        COUNT(*) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER (ORDER BY moved_at DESC) AS moved_at_dense_rank,
        SUM(qty) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_sum_2_prior,
        LAST_VALUE(unit_price) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
        ROUND(PERCENT_RANK() OVER w_all_by_unit_price_ties, 3) AS unit_price_percent_rank,
        NTH_VALUE(unit_price, 2) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        ROUND(AVG(unit_price) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_4,
        MAX(unit_price) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_max_2_prior
    FROM base
    WINDOW w_all_by_unit_price_ties AS (ORDER BY unit_price DESC)
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE moved_at_seq <= 2
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(moved_at) OVER (PARTITION BY move_type ORDER BY second_unit_price ASC, id) AS next_moved_at,
        ROUND(last_unit_price - AVG(last_unit_price) OVER (PARTITION BY moved_at_seq, warehouse_id), 2) AS last_unit_price_vs_moved_at_seq_warehouse_id_avg,
        NTH_VALUE(unit_price_avg_4, 2) OVER (PARTITION BY move_type, sku_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_avg_4,
        LEAD(qty, 3) OVER w_move_type_by_second_unit_pri AS next3_qty,
        DENSE_RANK() OVER w_sku_id_by_unit_price_max_2_p AS unit_price_max_2_prior_dense_rank,
        DENSE_RANK() OVER w_sku_id_by_unit_price_max_2_p_2 AS unit_price_max_2_prior_dense_rank_2
    FROM first_rows
    WINDOW w_move_type_by_second_unit_pri AS (PARTITION BY move_type ORDER BY second_unit_price ASC, id), w_sku_id_by_unit_price_max_2_p AS (PARTITION BY sku_id ORDER BY unit_price_max_2_prior DESC NULLS FIRST, id), w_sku_id_by_unit_price_max_2_p_2 AS (PARTITION BY sku_id ORDER BY unit_price_max_2_prior DESC NULLS FIRST)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN unit_price >= 330.0 THEN 'A' WHEN unit_price >= 209.4 THEN 'B' ELSE 'C' END AS unit_price_tier,
        warehouse_id,
        TO_DATE(moved_at) AS moved_at_day,
        ROUND(100 * COUNT(CASE WHEN move_type <> 'TRANSFER' THEN 1 END) / COUNT(*), 1) AS pct_move_type_transfer,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(moved_at) AS first_moved_at
    FROM windowed
    GROUP BY CASE WHEN unit_price >= 330.0 THEN 'A' WHEN unit_price >= 209.4 THEN 'B' ELSE 'C' END, warehouse_id, TO_DATE(moved_at)
    HAVING MAX(qty_sum_2_prior) > 301
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY pct_move_type_transfer NULLS LAST, unit_price_tier, warehouse_id, moved_at_day), 3) AS pct_move_type_transfer_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY first_moved_at DESC, unit_price_tier, warehouse_id, moved_at_day), 3) AS first_moved_at_percent_rank
    FROM rollup_base
)
SELECT
    unit_price_tier,
    distinct_ids,
    pct_move_type_transfer_cume_dist,
    ABS(distinct_ids - 4) AS distinct_ids_distance
FROM ranked
ORDER BY pct_move_type_transfer_cume_dist NULLS LAST
