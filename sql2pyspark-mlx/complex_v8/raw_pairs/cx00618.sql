WITH stacked AS (
    -- Stack the branches
    SELECT id AS key_id, unit_cost AS metric, 'sku' AS source
    FROM skus
    UNION ALL
    SELECT id AS key_id, weight_kg AS metric, 'sku_2' AS source
    FROM skus
)
SELECT source, COUNT(*) AS n_rows, SUM(metric) AS total_metric
FROM stacked
GROUP BY source
ORDER BY source
