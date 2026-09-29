WITH valid AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels
    FROM skus
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(*) AS move_count,
        COUNT(CASE WHEN move_type IN ('TRANSFER', 'OUTBOUND') THEN 1 END) AS move_move_type_count
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count
    FROM valid s
    LEFT JOIN move_stats st ON st.sku_id = s.id
),
exploded AS (
    -- One row per label
    SELECT
        s.id,
        s.sku_code,
        s.category,
        s.unit_cost,
        s.weight_kg,
        s.move_count,
        s.move_move_type_count,
        t.label_pos,
        t.label
    FROM profiled s
    LATERAL VIEW POSEXPLODE(s.labels) t AS label_pos, label
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM exploded s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.move_type IS NOT NULL
              AND st.operator = 'op_eve'
        )
),
totals AS (
    -- Group-level totals
    SELECT
        move_count,
        move_move_type_count,
        COUNT(*) FILTER (WHERE sku_code IS NOT NULL) AS n_sku_code_is_not_null,
        ARRAY_SORT(COLLECT_SET(category)) AS category_list,
        MIN(label_pos) AS min_label_pos,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(sku_code)) AS sku_code_list,
        ROUND(SUM(unit_cost) / NULLIF(SUM(weight_kg), 0), 3) AS unit_cost_to_weight_kg_ratio
    FROM filtered
    GROUP BY move_count, move_move_type_count
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER (ORDER BY row_count, move_count, move_move_type_count) AS row_count_quintile,
        MAX(move_move_type_count) OVER (PARTITION BY move_count, row_count) AS move_count_row_count_max_move_move_type_count,
        LAG(row_count) OVER (ORDER BY row_count, move_count, move_move_type_count) AS prev_row_count,
        ROUND(AVG(unit_cost_to_weight_kg_ratio) OVER (ORDER BY row_count, move_count, move_move_type_count ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS unit_cost_to_weight_kg_ratio_avg_6_prior
    FROM totals
)
-- Final projection
SELECT
    move_count,
    n_sku_code_is_not_null,
    category_list,
    min_label_pos,
    sku_code_list,
    unit_cost_to_weight_kg_ratio,
    row_count_quintile,
    unit_cost_to_weight_kg_ratio_avg_6_prior
FROM with_trends
WHERE SIZE(category_list) >= 1
ORDER BY unit_cost_to_weight_kg_ratio_avg_6_prior DESC NULLS FIRST
