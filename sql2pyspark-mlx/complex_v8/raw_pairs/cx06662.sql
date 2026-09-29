WITH valid_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        weight_kg,
        labels,
        is_active,
        CONCAT_WS(' / ', sku_code, category) AS sku_code_category_label,
        CASE WHEN unit_cost >= 251.2 THEN 'A' WHEN unit_cost >= 158.7 THEN 'B' ELSE 'C' END AS unit_cost_tier
    FROM skus
),
unit_cost_tier_stats AS (
    -- Median weight_kg per unit_cost_tier
    SELECT unit_cost_tier, PERCENTILE_APPROX(weight_kg, 0.5) AS median_weight_kg
    FROM valid_skus
    GROUP BY unit_cost_tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_weight_kg,
        ROUND(s.weight_kg - g.median_weight_kg, 2) AS weight_kg_minus_median
    FROM valid_skus s
    JOIN unit_cost_tier_stats g ON g.unit_cost_tier = s.unit_cost_tier
    WHERE s.weight_kg >= g.median_weight_kg
),
unit_cost_tier_stats_2 AS (
    -- Median weight_kg_minus_median per unit_cost_tier
    SELECT unit_cost_tier, PERCENTILE_APPROX(weight_kg_minus_median, 0.5) AS median_weight_kg_minus_median
    FROM compared
    GROUP BY unit_cost_tier
),
compared_2 AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_weight_kg_minus_median,
        ROUND(s.weight_kg_minus_median - g.median_weight_kg_minus_median, 2) AS weight_kg_minus_median_minus_median
    FROM compared s
    JOIN unit_cost_tier_stats_2 g ON g.unit_cost_tier = s.unit_cost_tier
    WHERE s.weight_kg_minus_median >= g.median_weight_kg_minus_median
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(median_weight_kg / NULLIF(SUM(median_weight_kg) OVER (PARTITION BY unit_cost_tier), 0), 4) AS median_weight_kg_share_of_unit_cost_tier,
        ROUND(weight_kg - AVG(weight_kg) OVER w_unit_cost_tier, 2) AS weight_kg_vs_unit_cost_tier_avg,
        weight_kg - LAG(weight_kg) OVER w_is_active_by_weight_kg_minus AS weight_kg_change,
        MAX(weight_kg_minus_median) OVER (ORDER BY weight_kg, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS weight_kg_minus_median_max_3_prior,
        RANK() OVER (ORDER BY weight_kg, id) AS weight_kg_rank,
        NTH_VALUE(weight_kg_minus_median, 2) OVER (ORDER BY weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_weight_kg_minus_median
    FROM compared_2
    WINDOW w_unit_cost_tier AS (PARTITION BY unit_cost_tier), w_is_active_by_weight_kg_minus AS (PARTITION BY is_active ORDER BY weight_kg_minus_median_minus_median DESC, id)
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.weight_kg,
        s.labels,
        s.is_active,
        s.median_weight_kg,
        s.weight_kg_minus_median_minus_median,
        s.median_weight_kg_share_of_unit_cost_tier,
        s.weight_kg_change,
        s.weight_kg_rank,
        s.second_weight_kg_minus_median,
        st.id AS move_id,
        st.qty AS move_qty,
        st.operator AS move_operator
    FROM ranked s
    JOIN stock_moves st ON st.sku_id = s.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, move_id, metric_name, amount
FROM (
    SELECT id, move_id, second_weight_kg_minus_median, weight_kg, weight_kg_change
    FROM line_level_moves
) src
UNPIVOT (
    amount FOR metric_name IN (second_weight_kg_minus_median AS second_weight_kg_minus_median_v, weight_kg AS m_weight_kg, weight_kg_change AS weight_kg_change_v)
) unp
ORDER BY id, move_id, metric_name
