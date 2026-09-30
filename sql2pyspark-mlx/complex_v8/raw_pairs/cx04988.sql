WITH cleaned_skus AS (
    -- Filter skus
    SELECT
        id,
        category,
        weight_kg,
        labels,
        is_active,
        LENGTH(TRIM(category)) AS category_len,
        unit_cost + weight_kg AS unit_cost_plus_weight_kg,
        SIZE(labels) AS labels_count
    FROM skus
    WHERE weight_kg >= 12.4
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(CASE WHEN operator <> 'op_ann' THEN 1 END) AS move_operator_count,
        COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_operator_count, 0) AS move_operator_count,
        COALESCE(st.move_count, 0) AS move_count
    FROM cleaned_skus s
    LEFT JOIN move_stats st ON st.sku_id = s.id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        category,
        weight_kg,
        labels,
        category_len,
        unit_cost_plus_weight_kg,
        move_operator_count,
        move_count,
        ROUND(category_len - AVG(category_len) OVER (PARTITION BY move_operator_count, labels_count), 2) AS category_len_vs_move_operator_count_labels_count,
        ROUND(labels_count / NULLIF(SUM(labels_count) OVER (PARTITION BY move_operator_count, labels_count), 0), 4) AS labels_count_share_of_move_operator_count_labels,
        SIZE(COLLECT_SET(category) OVER (PARTITION BY move_operator_count, labels_count)) AS distinct_category_in_move_operator_count_labels_
    FROM with_stats
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM metrics
    WHERE labels_count_share_of_move_operator_count_labels < (SELECT MAX(labels_count_share_of_move_operator_count_labels) FROM metrics)
),
stats AS (
    -- Group-level totals
    SELECT
        category,
        move_count,
        ROUND(SUM(labels_count_share_of_move_operator_count_labels) / NULLIF(SUM(move_count), 0), 3) AS labels_count_share_of_move_operator_count_labels_2,
        SUM(CASE WHEN category IN ('Beauty', 'Toys', 'Electronics') THEN unit_cost_plus_weight_kg ELSE 0 END) AS unit_cost_plus_weight_kg_category_in_beauty_toys
    FROM qualified
    GROUP BY category, move_count
)
SELECT category, move_count, metric_name, value
FROM (
    SELECT category, move_count, unit_cost_plus_weight_kg_category_in_beauty_toys, labels_count_share_of_move_operator_count_labels_2
    FROM stats
) src
UNPIVOT (
    value FOR metric_name IN (unit_cost_plus_weight_kg_category_in_beauty_toys, labels_count_share_of_move_operator_count_labels_2)
) unp
ORDER BY category, move_count, metric_name
