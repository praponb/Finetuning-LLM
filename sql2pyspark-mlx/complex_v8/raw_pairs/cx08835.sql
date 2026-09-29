WITH filtered_moves AS (
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
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE qty < 372
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM filtered_moves s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.move_type NOT IN ('INBOUND', 'TRANSFER')
              AND st.unit_price <= 293.2
        )
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE qty > (SELECT AVG(qty) FROM qualified)
),
with_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        s.days_since_moved_at,
        sk.labels AS sku_labels,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code
    FROM screened s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE ARRAY_CONTAINS(sk.labels, 'cold')
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    operator,
    days_since_moved_at,
    warehouse_city,
    warehouse_wh_code,
    CASE WHEN operator IN ('op_ann', 'op_eve') THEN 'group_a' WHEN operator = 'op_chan' THEN 'group_b' ELSE 'other' END AS operator_group,
    UPPER(TRIM(warehouse_city)) AS warehouse_city_clean
FROM with_details_sku
WHERE sku_id IS NOT NULL
ORDER BY warehouse_wh_code
