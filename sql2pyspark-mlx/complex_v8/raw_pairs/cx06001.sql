WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        LEAST(qty, unit_price) AS least_qty_unit_price,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 3)
      AND operator IN ('op_chan', 'op_ann')
),
enriched AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.least_qty_unit_price,
        s.moved_at_date,
        sk.sku_code,
        w.capacity AS warehouse_capacity
    FROM filtered s
    JOIN skus sk ON sk.id = s.sku_id AND LOWER(sk.sku_code) LIKE '%20%'
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE UPPER(TRIM(sk.sku_code)) LIKE 'E%'
),
filtered_2 AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND TO_DATE(st.moved_at) < DATE '2025-02-12'
              AND st.warehouse_id IS NOT NULL
        )
),
joined AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.least_qty_unit_price,
        s.moved_at_date,
        s.sku_code,
        sk.is_active AS sku_is_active,
        sk.weight_kg AS sku_weight_kg,
        sk.sku_code AS sku_sku_code
    FROM filtered_2 s
    LEFT JOIN skus sk ON sk.id = s.sku_id AND sk.weight_kg >= 8.5
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.qty > 85
              AND st.sku_id <= 15
        )
      AND s.sku_code IS NOT NULL
)
SELECT
    warehouse_id,
    sku_is_active,
    sku_id,
    GROUPING(warehouse_id) AS is_total_warehouse_id,
    ARRAY_SORT(COLLECT_SET(sku_code)) AS sku_code_list,
    COUNT(*) AS n_rows
FROM screened
GROUP BY ROLLUP(warehouse_id, sku_is_active, sku_id)
