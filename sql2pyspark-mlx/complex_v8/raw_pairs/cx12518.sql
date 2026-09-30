WITH valid AS (
    -- Scope the skus rows
    SELECT
        id,
        category,
        unit_cost,
        weight_kg,
        is_active,
        unit_cost - weight_kg AS unit_cost_minus_weight_kg,
        COALESCE(category, 'UNKNOWN') AS category_or_unknown
    FROM skus
    WHERE weight_kg BETWEEN 1.8 AND 21.1
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        category,
        unit_cost,
        weight_kg,
        is_active,
        category_or_unknown,
        MIN(weight_kg) OVER (PARTITION BY category, is_active ORDER BY unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_weight_kg_so_far,
        FIRST_VALUE(unit_cost) IGNORE NULLS OVER (PARTITION BY category, is_active ORDER BY unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_cost,
        ROUND(unit_cost / NULLIF(SUM(unit_cost) OVER (PARTITION BY category, category_or_unknown), 0), 4) AS unit_cost_share_of_category_category_or_unknown,
        SUM(unit_cost) OVER w_category_category_or_unknown AS category_category_or_unknown_sum_unit_cost,
        MAX(weight_kg) OVER w_category_category_or_unknown AS category_category_or_unknown_max_weight_kg,
        LAG(unit_cost) OVER (PARTITION BY is_active ORDER BY unit_cost_minus_weight_kg ASC, id) AS prev_unit_cost
    FROM valid
    WINDOW w_category_category_or_unknown AS (PARTITION BY category, category_or_unknown)
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.id) AS move_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.id) AS move_count_2,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.id) AS first_moved_at
    FROM metrics s
)
-- Subtotals with GROUPING SETS
SELECT
    move_count_2,
    category_or_unknown,
    GROUPING(move_count_2) AS is_total_move_count_2,
    ROUND(SUM(unit_cost), 2) AS total_unit_cost,
    ROUND(100 * COUNT(CASE WHEN NOT is_active THEN 1 END) / COUNT(*), 1) AS pct_not_is_active
FROM with_stats
GROUP BY GROUPING SETS ((move_count_2, category_or_unknown), (move_count_2), ())
