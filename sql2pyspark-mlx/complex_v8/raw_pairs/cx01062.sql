WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price
    FROM stock_moves
),
combined_details AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on
    FROM cleaned s
    JOIN warehouses w ON w.id = s.warehouse_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        operator,
        warehouse_capacity,
        warehouse_opened_on,
        CONCAT(move_type, '#', CAST(id AS STRING)) AS move_type_ref
    FROM combined_details
    WHERE warehouse_opened_on IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_warehouse_opened_on AS warehouse_opened_on_dense_rank,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER w_warehouse_opened_on_month, 2) AS warehouse_capacity_vs_warehouse_opened_on_month_,
        LAG(warehouse_capacity, 2) OVER w_all_by_warehouse_opened_on AS prev2_warehouse_capacity,
        ROUND(AVG(warehouse_capacity) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on)), 2) AS warehouse_opened_on_month_avg_warehouse_capacity
    FROM tagged
    WINDOW w_all_by_warehouse_opened_on AS (ORDER BY warehouse_opened_on DESC NULLS FIRST, id), w_warehouse_opened_on_month AS (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on))
)
-- Final projection
SELECT
    id,
    operator,
    warehouse_capacity,
    warehouse_opened_on_dense_rank,
    warehouse_capacity_vs_warehouse_opened_on_month_,
    prev2_warehouse_capacity,
    warehouse_opened_on_month_avg_warehouse_capacity,
    LEAST(warehouse_capacity, prev2_warehouse_capacity) AS least_warehouse_capacity_prev2_warehouse_capacit,
    prev2_warehouse_capacity - warehouse_capacity AS prev2_warehouse_capacity_minus_warehouse_capacit
FROM with_trends
WHERE warehouse_capacity_vs_warehouse_opened_on_month_ BETWEEN -10000.0 AND 14000.0
ORDER BY warehouse_opened_on_month_avg_warehouse_capacity
