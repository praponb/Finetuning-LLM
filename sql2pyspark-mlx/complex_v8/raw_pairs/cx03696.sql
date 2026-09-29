WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty,
        CONCAT(move_type, '#', CAST(warehouse_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-03-08'
),
joined_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.*,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city
    FROM scoped s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
warehouse_city_stats AS (
    -- Max warehouse_capacity per warehouse_city
    SELECT warehouse_city, MAX(warehouse_capacity) AS max_warehouse_capacity
    FROM joined_warehouse
    GROUP BY warehouse_city
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_warehouse_capacity,
        ROUND(s.warehouse_capacity - g.max_warehouse_capacity, 2) AS warehouse_capacity_minus_max
    FROM joined_warehouse s
    JOIN warehouse_city_stats g ON g.warehouse_city = s.warehouse_city
    WHERE s.warehouse_capacity = g.max_warehouse_capacity
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', warehouse_opened_on) AS warehouse_opened_on_month,
        PERCENTILE_APPROX(max_warehouse_capacity, 0.5) AS median_max_warehouse_capacity,
        MIN(max_warehouse_capacity) AS min_max_warehouse_capacity,
        MAX(warehouse_opened_on) AS last_warehouse_opened_on
    FROM outliers
    WHERE warehouse_capacity >= 2614
    GROUP BY DATE_TRUNC('MONTH', warehouse_opened_on)
    HAVING MAX(unit_price_per_qty) > 3.0
)
SELECT
    warehouse_opened_on_month,
    median_max_warehouse_capacity,
    min_max_warehouse_capacity,
    last_warehouse_opened_on
FROM summary
