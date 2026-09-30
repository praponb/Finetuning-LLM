WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price + qty AS unit_price_plus_qty
    FROM stock_moves
    WHERE unit_price >= 161.4
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.operator = 'op_bee') FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_operator_count,
        (SELECT COUNT(*) FILTER (WHERE st.operator IN ('op_ann', 'op_bee', 'op_dew')) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_operator_count_2,
        (SELECT MIN(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS min_move_qty
    FROM recent s
),
enriched AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_operator_count,
        s.move_operator_count_2,
        s.min_move_qty,
        sk.is_active AS sku_is_active,
        sk.weight_kg AS sku_weight_kg,
        sk.category AS sku_category,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city
    FROM profiled s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(move_operator_count, 0) AS move_operator_count_filled,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label,
        CONCAT(operator, '#', CAST(min_move_qty AS STRING)) AS operator_ref
    FROM enriched
),
with_parent AS (
    -- Attach sku attributes
    SELECT s.*, sk.sku_code, w.city AS warehouse_city_2
    FROM annotated s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    WHERE LOWER(sk.sku_code) LIKE '%p-%'
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        move_operator_count_2,
        min_move_qty,
        sku_is_active,
        sku_weight_kg,
        sku_category,
        moved_at_label,
        operator_ref,
        sku_code,
        CASE WHEN warehouse_city IN ('Kuala Lumpur', 'Bangkok') THEN 'group_a' WHEN warehouse_city = 'Singapore' THEN 'group_b' ELSE 'other' END AS warehouse_city_group,
        LAST_DAY(warehouse_opened_on) AS warehouse_opened_on_month_end
    FROM with_parent
    WHERE sku_category <> 'Apparel'
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count
    FROM derived s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, move_count, min_move_qty, move_operator_count_2
    FROM with_counts
) src
UNPIVOT (
    amount FOR measure IN (move_count, min_move_qty, move_operator_count_2)
) unp
