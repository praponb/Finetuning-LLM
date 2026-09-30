WITH valid_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        is_active,
        CASE WHEN weight_kg >= 20.8 THEN 'large' WHEN weight_kg >= 6.4 THEN 'medium' ELSE 'small' END AS weight_kg_tier,
        LENGTH(TRIM(category)) AS category_len
    FROM skus
    WHERE is_active = TRUE
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM valid_skus
    WHERE category_len > (SELECT MIN(category_len) FROM valid_skus)
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, COUNT(*) AS move_count, SUM(unit_price) AS total_move_unit_price
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        st.total_move_unit_price
    FROM with_activity s
    LEFT JOIN move_stats st ON st.sku_id = s.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_all_by_move_count_ties AS move_count_rank,
        ROUND(total_move_unit_price / NULLIF(SUM(total_move_unit_price) OVER (PARTITION BY category, weight_kg_tier), 0), 4) AS total_move_unit_price_share_of_category_weight_k,
        FIRST_VALUE(category_len) OVER (ORDER BY move_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_category_len,
        SUM(move_count) OVER (ORDER BY move_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_move_count
    FROM profiled
    WINDOW w_all_by_move_count_ties AS (ORDER BY move_count)
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.sku_code,
        s.category,
        s.unit_cost,
        s.is_active,
        s.weight_kg_tier,
        s.move_count,
        s.total_move_unit_price,
        s.move_count_rank,
        s.first_category_len,
        s.running_move_count,
        st.id AS move_id,
        st.warehouse_id AS move_warehouse_id,
        st.qty AS move_qty
    FROM metrics s
    LEFT JOIN stock_moves st ON st.sku_id = s.id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.id) AS last_moved_at,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.id) AS last_moved_at_2
    FROM detail_rows s
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_code,
        category,
        is_active,
        total_move_unit_price,
        move_count_rank,
        first_category_len,
        running_move_count,
        move_id,
        move_qty,
        last_moved_at,
        ROUND(first_category_len - AVG(first_category_len) OVER w_move_warehouse_id, 2) AS first_category_len_vs_move_warehouse_id_avg,
        ROUND(AVG(running_move_count) OVER (PARTITION BY move_count, is_active ORDER BY move_count ASC, id, move_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS running_move_count_avg_7,
        NTH_VALUE(move_qty, 2) OVER (PARTITION BY move_count, is_active ORDER BY move_count ASC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_move_qty,
        ROW_NUMBER() OVER (ORDER BY move_count_rank, id, move_id) AS move_count_rank_seq,
        ROW_NUMBER() OVER (PARTITION BY move_count, is_active ORDER BY move_count ASC, id, move_id) AS move_count_seq,
        MIN(move_count_rank) OVER (PARTITION BY move_count, is_active ORDER BY move_count ASC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_move_count_rank_so_far
    FROM with_counts
    WINDOW w_move_warehouse_id AS (PARTITION BY move_warehouse_id)
)
-- Subtotals with GROUPING SETS
SELECT
    move_count_rank,
    move_id,
    move_count_rank_seq,
    GROUPING_ID() AS gid,
    COUNT(*) AS cnt
FROM with_windows
GROUP BY GROUPING SETS ((move_count_rank, move_id, move_count_rank_seq), (move_count_rank), (move_id), ())
ORDER BY move_count_rank NULLS LAST, move_id NULLS LAST, move_count_rank_seq NULLS LAST
