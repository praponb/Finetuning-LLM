WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        unit_price - qty AS unit_price_minus_qty,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd') AS moved_at_label,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND sku_id >= 3
),
with_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_minus_qty,
        s.moved_at_label,
        s.moved_at_date,
        sk.is_active AS sku_is_active,
        sk.category AS sku_category,
        sk.labels AS sku_labels
    FROM valid_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_details
    WHERE unit_price > (SELECT AVG(unit_price) FROM with_details)
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.unit_price,
        s.moved_at_label,
        s.moved_at_date,
        s.sku_is_active,
        s.sku_category,
        s.sku_labels,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.operator AS move_operator,
        st.unit_price AS move_unit_price
    FROM filtered s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.moved_at_label,
        s.sku_is_active,
        s.sku_category,
        s.sku_labels,
        s.move_id,
        s.move_move_type,
        s.move_operator,
        s.move_unit_price,
        sk.unit_cost AS sku_unit_cost,
        sk.category AS sku_category_2,
        sk.weight_kg AS sku_weight_kg
    FROM detail_rows s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.unit_cost > 38.3
),
sku_category_stats AS (
    -- Avg sku_weight_kg per sku_category
    SELECT sku_category, AVG(sku_weight_kg) AS avg_sku_weight_kg
    FROM joined
    GROUP BY sku_category
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_sku_weight_kg,
        ROUND(s.sku_weight_kg - g.avg_sku_weight_kg, 2) AS sku_weight_kg_minus_avg
    FROM joined s
    JOIN sku_category_stats g ON g.sku_category = s.sku_category
    WHERE s.sku_weight_kg > g.avg_sku_weight_kg
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    moved_at_label,
    sku_is_active,
    sku_labels,
    move_id,
    move_move_type,
    move_operator,
    move_unit_price,
    sku_unit_cost,
    sku_category_2,
    sku_weight_kg_minus_avg
FROM vs_group
ORDER BY sku_category_2
