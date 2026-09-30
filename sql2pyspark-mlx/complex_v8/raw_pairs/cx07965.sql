WITH cleaned AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        is_active,
        CASE WHEN unit_cost >= 239.3 THEN 'large' WHEN unit_cost >= 108.5 THEN 'medium' ELSE 'small' END AS unit_cost_tier,
        SUBSTRING(TRIM(sku_code), 1, 2) AS sku_code_prefix,
        CASE WHEN unit_cost >= 277.0 THEN 'A' WHEN unit_cost >= 134.3 THEN 'B' ELSE 'C' END AS unit_cost_tier_2
    FROM skus
    WHERE (category = 'Toys' OR sku_code IS NOT NULL)
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.id) AS move_count,
        (SELECT COUNT(*) FILTER (WHERE st.operator = 'op_bee') FROM stock_moves st WHERE st.sku_id = s.id) AS move_operator_count
    FROM cleaned s
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY unit_cost_tier_2, category ORDER BY move_operator_count DESC) AS move_operator_count_dense_rank,
        RANK() OVER (PARTITION BY category, unit_cost_tier ORDER BY weight_kg ASC, id) AS weight_kg_rank,
        LAG(move_count, 2) OVER w_move_count_move_operator_cou AS prev2_move_count,
        ROUND(weight_kg / NULLIF(SUM(weight_kg) OVER (PARTITION BY unit_cost_tier_2), 0), 4) AS weight_kg_share_of_unit_cost_tier_2,
        MAX(move_count) OVER (PARTITION BY unit_cost_tier_2, category ORDER BY move_operator_count DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_move_count_so_far,
        ROUND(CUME_DIST() OVER (PARTITION BY category, unit_cost_tier ORDER BY weight_kg ASC), 3) AS weight_kg_cume_dist
    FROM summarized_rows
    WINDOW w_move_count_move_operator_cou AS (PARTITION BY move_count, move_operator_count ORDER BY move_operator_count, id)
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_code,
        category,
        is_active,
        sku_code_prefix,
        unit_cost_tier_2,
        move_operator_count,
        move_operator_count_dense_rank,
        weight_kg_rank,
        prev2_move_count,
        weight_kg_share_of_unit_cost_tier_2,
        SUM(move_count) OVER (PARTITION BY is_active, unit_cost_tier ORDER BY max_move_count_so_far DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_move_count,
        COUNT(*) OVER (PARTITION BY is_active, unit_cost_tier ORDER BY max_move_count_so_far DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(unit_cost - AVG(unit_cost) OVER w_is_active_move_operator_coun, 2) AS unit_cost_vs_is_active_move_operator_count_dense,
        MAX(max_move_count_so_far) OVER (PARTITION BY is_active, unit_cost_tier ORDER BY max_move_count_so_far DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS max_move_count_so_far_max_6_prior,
        NTH_VALUE(prev2_move_count, 2) OVER (PARTITION BY is_active, unit_cost_tier ORDER BY max_move_count_so_far DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev2_move_count
    FROM sequenced
    WINDOW w_is_active_move_operator_coun AS (PARTITION BY is_active, move_operator_count_dense_rank)
)
-- Subtotals with GROUPING SETS
SELECT
    weight_kg_rank,
    move_operator_count_dense_rank,
    prev2_move_count,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT category) AS distinct_categorys
FROM sequenced_2
GROUP BY GROUPING SETS ((weight_kg_rank, move_operator_count_dense_rank, prev2_move_count), (weight_kg_rank))
ORDER BY weight_kg_rank NULLS LAST, move_operator_count_dense_rank NULLS LAST, prev2_move_count NULLS LAST
