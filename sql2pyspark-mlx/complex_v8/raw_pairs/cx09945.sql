WITH filtered AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        CONCAT_WS(' / ', sku_code, category) AS sku_code_category_label
    FROM skus
    WHERE is_active = TRUE
      AND weight_kg > 8.5
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.warehouse_id IS NOT NULL
              AND st.unit_price <= 363.6
        )
      AND s.sku_code_category_label IS NOT NULL
),
deduped AS (
    -- Keep the latest row per category
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        sku_code_category_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY unit_cost DESC, id DESC) AS row_num
        FROM qualified
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT DISTINCT id
FROM deduped
ORDER BY id
