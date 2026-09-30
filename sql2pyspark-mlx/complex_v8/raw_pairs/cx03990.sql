WITH filtered AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        COALESCE(unit_cost, 0.0) AS unit_cost_filled
    FROM skus
    WHERE unit_cost < 251.2
      AND is_active = FALSE
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY unit_cost, id) AS unit_cost_rank,
        DENSE_RANK() OVER (ORDER BY unit_cost) AS unit_cost_dense_rank,
        SUM(unit_cost_filled) OVER (PARTITION BY is_active) AS is_active_sum_unit_cost_filled
    FROM filtered
)
-- Subtotals with CUBE
SELECT
    unit_cost_dense_rank,
    is_active,
    GROUPING(unit_cost_dense_rank) AS is_total_unit_cost_dense_rank,
    ROUND(AVG(is_active_sum_unit_cost_filled), 2) AS avg_is_active_sum_unit_cost_filled
FROM ranked
GROUP BY CUBE(unit_cost_dense_rank, is_active)
ORDER BY unit_cost_dense_rank NULLS LAST, is_active NULLS LAST
