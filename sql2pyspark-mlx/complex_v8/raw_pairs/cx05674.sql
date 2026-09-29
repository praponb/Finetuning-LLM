WITH base AS (
    -- Filter skus
    SELECT
        id,
        category,
        weight_kg,
        labels,
        CASE WHEN category IN ('Apparel', 'Food') THEN 'group_a' WHEN category = 'Toys' THEN 'group_b' ELSE 'other' END AS category_group,
        weight_kg + unit_cost AS weight_kg_plus_unit_cost,
        ROUND(unit_cost * 1.1, 2) AS unit_cost_adj
    FROM skus
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (ORDER BY weight_kg_plus_unit_cost DESC NULLS FIRST) AS weight_kg_plus_unit_cost_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY category_group ORDER BY weight_kg DESC), 3) AS weight_kg_cume_dist,
        ROW_NUMBER() OVER (ORDER BY weight_kg_plus_unit_cost DESC NULLS FIRST, id) AS rn,
        SIZE(COLLECT_SET(category) OVER (PARTITION BY category_group)) AS distinct_category_in_category_group,
        MAX(weight_kg) OVER w_category_group AS category_group_max_weight_kg,
        NTH_VALUE(unit_cost_adj, 2) OVER (ORDER BY weight_kg_plus_unit_cost DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_cost_adj
    FROM base
    WINDOW w_category_group AS (PARTITION BY category_group)
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM windowed s
    WHERE s.id IN (
            SELECT st.sku_id
            FROM stock_moves st
        )
      AND s.category_group_max_weight_kg >= 50.1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(unit_cost_adj) OVER (PARTITION BY rn, distinct_category_in_category_group) AS rn_distinct_category_in_category_group_max_unit_,
        FIRST_VALUE(weight_kg_plus_unit_cost) OVER (PARTITION BY category_group ORDER BY distinct_category_in_category_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_weight_kg_plus_unit_cost,
        NTH_VALUE(second_unit_cost_adj, 2) OVER (PARTITION BY category_group ORDER BY distinct_category_in_category_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_unit_cost_adj,
        ROUND(unit_cost_adj - AVG(unit_cost_adj) OVER (PARTITION BY rn, distinct_category_in_category_group), 2) AS unit_cost_adj_vs_rn_distinct_category_in_categor,
        ROUND(CUME_DIST() OVER (PARTITION BY category_group ORDER BY distinct_category_in_category_group), 3) AS distinct_category_in_category_group_cume_dist
    FROM without_matches
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        category,
        labels,
        category_group,
        weight_kg_plus_unit_cost_rank,
        weight_kg_cume_dist,
        rn,
        distinct_category_in_category_group,
        category_group_max_weight_kg,
        second_unit_cost_adj,
        rn_distinct_category_in_category_group_max_unit_,
        first_weight_kg_plus_unit_cost,
        second_second_unit_cost_adj,
        unit_cost_adj_vs_rn_distinct_category_in_categor,
        distinct_category_in_category_group_cume_dist,
        CONCAT(category, '#', CAST(rn AS STRING)) AS category_ref,
        ABS(weight_kg_cume_dist - 0.5) AS weight_kg_cume_dist_distance,
        UPPER(TRIM(category_group)) AS category_group_clean
    FROM metrics
    WHERE ARRAY_CONTAINS(labels, 'cold')
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND HOUR(st.moved_at) BETWEEN 10 AND 15
        )
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN category IN ('Apparel', 'Food') THEN 'group_a' WHEN category = 'Toys' THEN 'group_b' ELSE 'other' END AS category_group_2
    FROM filtered
    WHERE rn < 9
)
SELECT
    id,
    labels,
    category_group,
    weight_kg_plus_unit_cost_rank,
    weight_kg_cume_dist,
    rn,
    distinct_category_in_category_group,
    category_group_max_weight_kg,
    second_unit_cost_adj,
    rn_distinct_category_in_category_group_max_unit_,
    first_weight_kg_plus_unit_cost,
    unit_cost_adj_vs_rn_distinct_category_in_categor,
    distinct_category_in_category_group_cume_dist,
    weight_kg_cume_dist_distance,
    category_group_clean,
    category_group_2,
    LOWER(REGEXP_REPLACE(TRIM(category_group_2), ' ', '_')) AS category_group_2_slug
FROM scored
ORDER BY category_group DESC NULLS FIRST, id DESC
