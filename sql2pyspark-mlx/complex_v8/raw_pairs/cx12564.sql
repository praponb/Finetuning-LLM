WITH base AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        is_active,
        COALESCE(category, 'UNKNOWN') AS category_or_unknown
    FROM skus
    WHERE unit_cost >= 38.3
      AND is_active = TRUE
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_code,
        category_or_unknown,
        ROUND(weight_kg - AVG(weight_kg) OVER w_category_is_active, 2) AS weight_kg_vs_category_is_active_avg,
        FIRST_VALUE(weight_kg) IGNORE NULLS OVER (PARTITION BY is_active, category_or_unknown ORDER BY unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_weight_kg,
        unit_cost - LAG(unit_cost) OVER (PARTITION BY is_active, category_or_unknown ORDER BY unit_cost DESC, id) AS unit_cost_change
    FROM base
    WINDOW w_category_is_active AS (PARTITION BY category, is_active)
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.qty > 199
        )
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND MONTH(st.moved_at) IN (2, 3)
              AND st.move_type IN ('OUTBOUND', 'INBOUND', 'ADJUST')
        )
)
SELECT
    id,
    sku_code,
    category_or_unknown,
    LENGTH(TRIM(sku_code)) AS sku_code_len
FROM screened
WHERE first_known_weight_kg >= 8.5
