WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at,
        CONCAT(move_type, '#', CAST(qty AS STRING)) AS move_type_ref,
        UPPER(TRIM(operator)) AS operator_clean
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND operator = 'op_chan'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        qty,
        operator,
        days_since_moved_at,
        move_type_ref,
        operator_clean,
        FIRST_VALUE(days_since_moved_at) OVER (PARTITION BY sku_id, warehouse_id ORDER BY days_since_moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_days_since_moved_at,
        COUNT(*) OVER (PARTITION BY warehouse_id, operator) AS rows_in_warehouse_id_operator
    FROM prepared_moves
),
with_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.sku_id,
        s.qty,
        s.operator,
        s.days_since_moved_at,
        s.move_type_ref,
        s.operator_clean,
        sk.category AS sku_category,
        sk.labels AS sku_labels,
        sk.sku_code
    FROM sequenced s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE SIZE(sk.labels) >= 1
),
with_parent_sku AS (
    -- Attach sku attributes
    SELECT
        s.*,
        sk.is_active AS sku_is_active,
        sk.weight_kg AS sku_weight_kg,
        sk.unit_cost AS sku_unit_cost
    FROM with_details_sku s
    JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    sku_category,
    operator,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE UPPER(TRIM(sku_code)) LIKE 'A%') AS n_upper_trim_sku_code_like_a
FROM with_parent_sku
GROUP BY ROLLUP(sku_category, operator)
