WITH scoped_moves AS (
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
        unit_price - qty AS unit_price_minus_qty,
        LENGTH(TRIM(move_type)) AS move_type_len,
        LOWER(TRIM(operator)) AS operator_clean
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-24 09:00:00'
),
with_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_minus_qty,
        s.move_type_len,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city
    FROM scoped_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE w.capacity < 11506
),
enriched_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.*,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity_2
    FROM with_details s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
joined_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_minus_qty,
        s.move_type_len,
        s.warehouse_capacity,
        s.warehouse_city,
        s.warehouse_opened_on,
        s.warehouse_wh_code,
        s.warehouse_capacity_2,
        w.city AS warehouse_city_2
    FROM enriched_warehouse s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM joined_warehouse s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND TO_DATE(st.moved_at) < DATE '2025-02-18'
              AND st.qty >= 168
        )
      AND s.move_type_len < 11
),
filtered AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM screened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND LENGTH(TRIM(w.wh_code)) >= 6
              AND YEAR(w.opened_on) = 2024
        )
)
-- Subtotals with GROUPING SETS
SELECT
    operator,
    warehouse_city,
    GROUPING_ID() AS gid,
    MIN(warehouse_opened_on) AS first_warehouse_opened_on,
    PERCENTILE_APPROX(unit_price, 0.5) AS median_unit_price,
    MIN(move_type_len) AS min_move_type_len
FROM filtered
GROUP BY GROUPING SETS ((operator, warehouse_city), (operator))
