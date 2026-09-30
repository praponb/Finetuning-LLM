WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        moved_at + INTERVAL 2 HOURS AS moved_at_plus_2h,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug_2
    FROM stock_moves
    WHERE move_type IN ('ADJUST', 'TRANSFER', 'INBOUND')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(moved_at_plus_2h) OVER (ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at_plus_2h,
        ROUND(AVG(qty) OVER (ORDER BY moved_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS qty_avg_4,
        ROUND(PERCENT_RANK() OVER w_all_by_moved_at_ties, 3) AS moved_at_percent_rank,
        RANK() OVER w_all_by_moved_at AS moved_at_rank,
        NTH_VALUE(qty, 2) OVER (ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        RANK() OVER (ORDER BY moved_at) AS moved_at_rank_2
    FROM base
    WINDOW w_all_by_moved_at_ties AS (ORDER BY moved_at), w_all_by_moved_at AS (ORDER BY moved_at, id)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        move_type_slug,
        moved_at_plus_2h,
        move_type_slug_2,
        first_moved_at_plus_2h,
        qty_avg_4,
        moved_at_percent_rank,
        moved_at_rank,
        second_qty
    FROM with_trends
    WHERE moved_at_rank_2 <= 3
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS first_moved_at,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at_2
    FROM leaders s
)
SELECT
    sku_id,
    moved_at_rank,
    operator,
    COUNT(*) AS n_rows,
    MAX(last_moved_at_2) AS last_last_moved_at_2
FROM summarized_rows
GROUP BY ROLLUP(sku_id, moved_at_rank, operator)
ORDER BY sku_id NULLS LAST, moved_at_rank NULLS LAST, operator NULLS LAST
