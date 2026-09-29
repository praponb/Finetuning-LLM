WITH scoped AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        IF(is_active, 'yes', 'no') AS is_active_label,
        ROUND(weight_kg * 0.25, 2) AS weight_kg_adj,
        ARRAY_JOIN(ARRAY_SORT(labels), ',') AS labels_text
    FROM skus
    WHERE sku_code IS NOT NULL
      AND category = 'Toys'
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.qty >= 67
        )
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(weight_kg_adj - 22.5) AS weight_kg_adj_distance,
        CASE WHEN is_active THEN 1 ELSE 0 END AS is_active_flag
    FROM without_matches
    WHERE unit_cost >= 108.5
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.id) AS first_moved_at,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.id) AS last_moved_at
    FROM enriched s
),
without_matches_2 AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_counts s
    WHERE s.id IN (
            SELECT st.sku_id
            FROM stock_moves st
        )
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_unit_cost, 3) AS unit_cost_percent_rank,
        LEAD(weight_kg, 2) OVER w_all_by_unit_cost AS next2_weight_kg,
        COUNT(*) OVER w_is_active_label_is_active AS rows_in_is_active_label_is_active,
        ROUND(PERCENT_RANK() OVER w_all_by_unit_cost_ties, 3) AS unit_cost_percent_rank_2,
        ROUND(CUME_DIST() OVER (ORDER BY unit_cost), 3) AS unit_cost_cume_dist
    FROM without_matches_2
    WINDOW w_all_by_unit_cost AS (ORDER BY unit_cost, id), w_is_active_label_is_active AS (PARTITION BY is_active_label, is_active), w_all_by_unit_cost_ties AS (ORDER BY unit_cost)
)
-- Final projection
SELECT
    id,
    sku_code,
    category,
    unit_cost,
    labels,
    is_active,
    is_active_label,
    weight_kg_adj,
    is_active_flag,
    last_moved_at,
    unit_cost_percent_rank,
    next2_weight_kg,
    unit_cost_percent_rank_2,
    unit_cost_cume_dist,
    TO_DATE(first_moved_at) AS first_moved_at_date
FROM metrics
WHERE unit_cost_cume_dist > 0.5
ORDER BY unit_cost_percent_rank NULLS LAST, id DESC
