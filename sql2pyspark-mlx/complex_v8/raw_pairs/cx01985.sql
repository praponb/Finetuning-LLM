WITH scoped_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        labels
    FROM skus
    WHERE sku_code IS NOT NULL
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN st.operator = 'op_eve' THEN 1 END) FROM stock_moves st WHERE st.sku_id = s.id) AS move_operator_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.id) AS move_count,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.id) AS first_moved_at
    FROM scoped_skus s
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN category IN ('Toys', 'Apparel') THEN 'group_a' WHEN category = 'Beauty' THEN 'group_b' ELSE 'other' END AS category_group,
        CASE WHEN move_operator_count >= 4 THEN 'HIGH' WHEN move_operator_count >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS move_operator_count_tier,
        weight_kg - move_operator_count AS weight_kg_minus_move_operator_count
    FROM with_counts
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        weight_kg,
        first_moved_at,
        category_group,
        move_operator_count_tier,
        DENSE_RANK() OVER (ORDER BY weight_kg, id) AS weight_kg_dense_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY weight_kg), 3) AS weight_kg_percent_rank,
        MAX(weight_kg) OVER (ORDER BY weight_kg, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS weight_kg_max_6_prior,
        ROUND(move_operator_count / NULLIF(SUM(move_operator_count) OVER w_category_first_moved_at_to_d, 0), 4) AS move_operator_count_share_of_category_first_move
    FROM labeled
    WINDOW w_category_first_moved_at_to_d AS (PARTITION BY category, TO_DATE(first_moved_at))
)
SELECT
    id,
    weight_kg,
    first_moved_at,
    category_group,
    weight_kg_dense_rank,
    weight_kg_percent_rank,
    weight_kg_max_6_prior,
    ROUND(weight_kg / NULLIF(weight_kg_max_6_prior, 0), 3) AS weight_kg_per_weight_kg_max_6_prior
FROM sequenced
WHERE move_operator_count_share_of_category_first_move < 0.8
ORDER BY weight_kg_per_weight_kg_max_6_prior DESC NULLS FIRST
