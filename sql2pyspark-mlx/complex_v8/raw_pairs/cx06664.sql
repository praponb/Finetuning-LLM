WITH base_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        CASE WHEN is_active THEN 1 ELSE 0 END AS is_active_flag
    FROM skus
    WHERE weight_kg > 4.7
),
unioned AS (
    -- Stack the branches
    SELECT 'sku' AS origin, id AS qty, category AS label
    FROM base_skus
    WHERE is_active = TRUE
    UNION ALL
    SELECT 'warehouse' AS origin, id AS qty, wh_code AS label
    FROM warehouses
    WHERE opened_on BETWEEN DATE '2024-04-13' AND DATE '2025-02-14'
    UNION ALL
    SELECT 'sku_3' AS origin, id AS qty, category AS label
    FROM skus
)
SELECT origin, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM unioned
GROUP BY origin
ORDER BY origin
