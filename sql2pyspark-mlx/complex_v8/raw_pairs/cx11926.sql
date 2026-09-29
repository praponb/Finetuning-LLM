WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels
    FROM skus
    WHERE weight_kg <= 21.0
),
category_stats AS (
    -- Avg weight_kg per category
    SELECT category, AVG(weight_kg) AS avg_weight_kg
    FROM filtered
    GROUP BY category
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_weight_kg,
        ROUND(s.weight_kg - g.avg_weight_kg, 2) AS weight_kg_minus_avg
    FROM filtered s
    JOIN category_stats g ON g.category = s.category
    WHERE s.weight_kg > g.avg_weight_kg
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        labels,
        avg_weight_kg,
        weight_kg_minus_avg,
        ROUND(AVG(unit_cost) OVER (PARTITION BY category ORDER BY weight_kg DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_cost_avg_3_prior,
        MIN(avg_weight_kg) OVER (PARTITION BY category) AS category_min_avg_weight_kg
    FROM above_average
)
SELECT
    id,
    sku_code,
    category,
    weight_kg,
    labels,
    avg_weight_kg,
    weight_kg_minus_avg,
    unit_cost_avg_3_prior,
    COALESCE(category, 'UNKNOWN') AS category_or_unknown,
    LOWER(TRIM(category)) AS category_clean
FROM metrics
