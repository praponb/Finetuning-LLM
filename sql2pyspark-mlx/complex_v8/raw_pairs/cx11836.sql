WITH cleaned_moves AS (
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
        GREATEST(unit_price, qty) AS greatest_unit_price_qty
    FROM stock_moves
    WHERE move_type NOT IN ('OUTBOUND', 'ADJUST')
      AND unit_price > 130.6
),
combined_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost,
        sk.labels AS sku_labels
    FROM cleaned_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
grouped AS (
    -- Group-level totals
    SELECT
        warehouse_id,
        operator,
        MAX(moved_at) AS last_moved_at,
        COUNT(DISTINCT operator) AS distinct_operators,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(sku_code)) AS sku_code_list,
        MIN(sku_unit_cost) AS min_sku_unit_cost,
        MAX(sku_unit_cost) AS max_sku_unit_cost
    FROM combined_details
    GROUP BY warehouse_id, operator
    HAVING COUNT(*) >= 3
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS first_moved_at
    FROM grouped s
),
with_parent AS (
    -- Attach warehouse attributes
    SELECT
        s.warehouse_id,
        s.operator,
        s.last_moved_at,
        s.distinct_operators,
        s.n_rows,
        s.sku_code_list,
        s.min_sku_unit_cost,
        s.first_moved_at,
        w.capacity AS warehouse_capacity
    FROM with_stats s
    JOIN warehouses w ON w.id = s.warehouse_id
),
with_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.*,
        w.capacity AS warehouse_capacity_2,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city
    FROM with_parent s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
combined_details_2 AS (
    -- Join warehouses
    SELECT s.*, w.opened_on AS warehouse_opened_on_2, w.city AS warehouse_city_2
    FROM with_details_warehouse s
    JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    warehouse_id,
    operator,
    distinct_operators,
    sku_code_list,
    min_sku_unit_cost,
    first_moved_at,
    warehouse_opened_on,
    warehouse_city,
    warehouse_city_2,
    ARRAY_JOIN(ARRAY_SORT(sku_code_list), ',') AS sku_code_list_text,
    CASE WHEN operator IN ('op_eve', 'op_chan') THEN 'group_a' WHEN operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS operator_group
FROM combined_details_2
