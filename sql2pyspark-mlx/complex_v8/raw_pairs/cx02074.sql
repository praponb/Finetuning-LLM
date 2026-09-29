WITH valid_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        IF(is_active, 'yes', 'no') AS is_active_label
    FROM skus
    WHERE weight_kg BETWEEN 1.8 AND 14.6
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        labels,
        ROUND(weight_kg / NULLIF(SUM(weight_kg) OVER (PARTITION BY is_active_label), 0), 4) AS weight_kg_share_of_is_active_label,
        MAX(weight_kg) OVER (ORDER BY unit_cost NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS weight_kg_max_4,
        LAST_VALUE(weight_kg) IGNORE NULLS OVER (PARTITION BY category, is_active_label ORDER BY weight_kg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_weight_kg,
        LAG(weight_kg) OVER w_category_is_active_label_by_2 AS prev_weight_kg,
        RANK() OVER (PARTITION BY category, is_active_label ORDER BY weight_kg DESC) AS weight_kg_rank,
        LAST_VALUE(weight_kg) IGNORE NULLS OVER (PARTITION BY category, is_active_label ORDER BY weight_kg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_weight_kg_2
    FROM valid_skus
    WINDOW w_category_is_active_label_by_2 AS (PARTITION BY category, is_active_label ORDER BY weight_kg ASC, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        sku_code,
        unit_cost,
        labels,
        weight_kg_rank,
        last_known_weight_kg_2,
        COALESCE(weight_kg_rank, 0) AS weight_kg_rank_filled,
        ROUND(last_known_weight_kg_2 * 0.25, 2) AS last_known_weight_kg_2_adj
    FROM with_windows
    WHERE last_known_weight_kg < 14.6
),
annotated_2 AS (
    -- Add calculated columns
    SELECT
        id,
        sku_code,
        unit_cost,
        labels,
        weight_kg_rank,
        last_known_weight_kg_2,
        weight_kg_rank_filled,
        last_known_weight_kg_2_adj,
        COALESCE(last_known_weight_kg_2, 0.0) AS last_known_weight_kg_2_filled,
        GREATEST(weight_kg_rank, weight_kg_rank_filled) AS greatest_weight_kg_rank_weight_kg_rank_filled,
        UPPER(TRIM(sku_code)) AS sku_code_clean
    FROM annotated
)
-- Subtotals with GROUPING SETS
SELECT
    weight_kg_rank,
    greatest_weight_kg_rank_weight_kg_rank_filled,
    weight_kg_rank_filled,
    ROUND(SUM(greatest_weight_kg_rank_weight_kg_rank_filled) / NULLIF(SUM(unit_cost), 0), 3) AS greatest_weight_kg_rank_weight_kg_rank_filled_to,
    ROUND(SUM(last_known_weight_kg_2_filled), 2) AS total_last_known_weight_kg_2_filled
FROM annotated_2
GROUP BY GROUPING SETS ((weight_kg_rank, greatest_weight_kg_rank_weight_kg_rank_filled, weight_kg_rank_filled), (weight_kg_rank), ())
ORDER BY weight_kg_rank NULLS LAST, greatest_weight_kg_rank_weight_kg_rank_filled NULLS LAST, weight_kg_rank_filled NULLS LAST
