WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour
    FROM stock_moves
    WHERE qty BETWEEN 137 AND 271
      AND warehouse_id <= 5
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        sku_id,
        qty,
        operator,
        unit_price,
        moved_at_hour,
        SUBSTRING(TRIM(operator), 1, 4) AS operator_prefix
    FROM cleaned
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.sku_id,
        s.qty,
        s.operator,
        s.moved_at_hour,
        s.operator_prefix,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost
    FROM derived s
    JOIN skus sk ON sk.id = s.sku_id
),
sku_id_stats AS (
    -- Max sku_unit_cost per sku_id
    SELECT sku_id, MAX(sku_unit_cost) AS max_sku_unit_cost
    FROM with_details
    GROUP BY sku_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_sku_unit_cost,
        ROUND(s.sku_unit_cost - g.max_sku_unit_cost, 2) AS sku_unit_cost_minus_max
    FROM with_details s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.sku_unit_cost = g.max_sku_unit_cost
),
combined_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.sku_id,
        s.operator,
        s.moved_at_hour,
        s.sku_code,
        s.sku_unit_cost,
        s.max_sku_unit_cost,
        sk.unit_cost AS sku_unit_cost_2,
        sk.weight_kg AS sku_weight_kg
    FROM vs_group s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.unit_cost < 239.3
)
SELECT
    id,
    sku_id,
    operator,
    moved_at_hour,
    sku_code,
    sku_unit_cost,
    max_sku_unit_cost,
    QUARTER(moved_at_hour) AS moved_at_hour_quarter,
    TO_DATE(moved_at_hour) AS moved_at_hour_date
FROM combined_details
