WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        ABS(unit_price - 258.6) AS unit_price_distance
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
enriched_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        s.unit_price_distance,
        sk.weight_kg AS sku_weight_kg,
        sk.sku_code
    FROM base_moves s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.weight_kg > 4.7
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM enriched_sku s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.qty < 387
        )
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(unit_price_distance) IGNORE NULLS OVER (ORDER BY sku_weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_distance,
        DENSE_RANK() OVER (ORDER BY sku_weight_kg) AS sku_weight_kg_dense_rank,
        COUNT(*) OVER (PARTITION BY operator, warehouse_id) AS rows_in_operator_warehouse_id,
        LAG(sku_weight_kg) OVER w_all_by_sku_weight_kg AS prev_sku_weight_kg,
        ROUND(unit_price_distance - AVG(unit_price_distance) OVER (PARTITION BY operator, warehouse_id), 2) AS unit_price_distance_vs_operator_warehouse_id_avg
    FROM filtered
    WINDOW w_all_by_sku_weight_kg AS (ORDER BY sku_weight_kg, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        operator,
        unit_price_distance,
        sku_weight_kg,
        first_known_unit_price_distance,
        sku_weight_kg_dense_rank,
        unit_price_distance_vs_operator_warehouse_id_avg,
        sku_weight_kg + unit_price_distance AS sku_weight_kg_plus_unit_price_distance,
        ROUND(sku_weight_kg / NULLIF(unit_price_distance, 0), 3) AS sku_weight_kg_per_unit_price_distance,
        CONCAT(sku_code, '#', CAST(sku_weight_kg_dense_rank AS STRING)) AS sku_code_ref
    FROM with_trends
    WHERE UPPER(TRIM(sku_code)) LIKE 'T%'
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count
    FROM annotated s
)
SELECT *
FROM (
    SELECT move_count, sku_id, operator, unit_price_distance
    FROM with_counts
) src
PIVOT (
    MIN(unit_price_distance)
    FOR operator IN ('op_bee', 'op_dew', 'op_eve')
)
ORDER BY move_count, sku_id
