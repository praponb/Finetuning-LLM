WITH cleaned_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels
    FROM skus
    WHERE is_active = FALSE
),
deduped AS (
    -- Keep the latest row per category
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY weight_kg DESC, id DESC) AS recency_rank
        FROM cleaned_skus
    ) ranked
    WHERE recency_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, unit_cost, weight_kg
    FROM deduped
) src
UNPIVOT (
    metric_value FOR field IN (unit_cost, weight_kg)
) unp
