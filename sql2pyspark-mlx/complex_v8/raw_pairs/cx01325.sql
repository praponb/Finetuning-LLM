WITH base AS (
    -- Filter skus
    SELECT *
    FROM skus
    WHERE LOWER(sku_code) LIKE '%08%'
),
annotated AS (
    -- Add calculated columns
    SELECT *, LOWER(TRIM(category)) AS category_clean
    FROM base
    WHERE is_active = FALSE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        is_active,
        category_clean,
        LAG(weight_kg, 1, 0) OVER w_category_by_unit_cost AS prev_weight_kg,
        COUNT(*) OVER (PARTITION BY is_active) AS rows_in_is_active
    FROM annotated
    WINDOW w_category_by_unit_cost AS (PARTITION BY category ORDER BY unit_cost DESC, id)
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM metrics
    WHERE unit_cost < (SELECT MAX(unit_cost) FROM metrics)
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM without_matches s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.move_type IS NOT NULL
        )
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM qualified s
    LEFT JOIN move_stats st ON st.sku_id = s.id
)
SELECT
    id,
    sku_code,
    unit_cost,
    weight_kg,
    is_active,
    category_clean,
    prev_weight_kg,
    rows_in_is_active,
    move_count
FROM summarized_rows
ORDER BY prev_weight_kg DESC
