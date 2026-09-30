WITH base_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        is_active,
        CONCAT_WS(' / ', sku_code, category) AS sku_code_category_label
    FROM skus
    WHERE weight_kg > 6.4
      AND sku_code IS NOT NULL
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN category IN ('Food', 'Apparel') THEN 'group_a' WHEN category = 'Electronics' THEN 'group_b' ELSE 'other' END AS category_group,
        SUBSTRING(TRIM(category), 1, 3) AS category_prefix
    FROM base_skus
    WHERE sku_code_category_label IS NOT NULL
),
category_group_stats AS (
    -- Avg weight_kg per category_group
    SELECT category_group, AVG(weight_kg) AS avg_weight_kg
    FROM tagged
    GROUP BY category_group
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_weight_kg,
        ROUND(s.weight_kg - g.avg_weight_kg, 2) AS weight_kg_minus_avg
    FROM tagged s
    JOIN category_group_stats g ON g.category_group = s.category_group
    WHERE s.weight_kg > g.avg_weight_kg
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(*) AS move_count,
        SUM(qty) AS total_move_qty,
        COUNT(*) AS move_count_2
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        st.total_move_qty,
        COALESCE(st.move_count_2, 0) AS move_count_2
    FROM vs_group s
    LEFT JOIN move_stats st ON st.sku_id = s.id
)
-- Subtotals with CUBE
SELECT
    is_active,
    move_count,
    GROUPING(is_active) AS is_total_is_active,
    ARRAY_SORT(COLLECT_SET(category)) AS category_list,
    ROUND(SUM(total_move_qty) / NULLIF(SUM(move_count_2), 0), 3) AS total_move_qty_to_move_count_2_ratio,
    COUNT(DISTINCT id) AS distinct_ids
FROM summarized_rows
GROUP BY CUBE(is_active, move_count)
