WITH valid_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active
    FROM skus
    WHERE sku_code IS NOT NULL
      AND category = 'Beauty'
)
SELECT id AS ref_id, unit_cost
FROM valid_skus
UNION ALL
SELECT warehouse_id AS ref_id, unit_price AS unit_cost
FROM stock_moves
