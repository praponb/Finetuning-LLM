WITH filtered_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        labels,
        ROUND(unit_cost / NULLIF(weight_kg, 0), 3) AS unit_cost_per_weight_kg
    FROM skus
    WHERE unit_cost < 239.3
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.unit_price) FROM stock_moves st WHERE st.sku_id = s.id) AS min_move_unit_price
    FROM filtered_skus s
)
SELECT DISTINCT unit_cost, id
FROM profiled
ORDER BY id DESC
