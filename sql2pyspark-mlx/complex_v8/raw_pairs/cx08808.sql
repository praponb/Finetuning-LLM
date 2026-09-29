WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE warehouse_id <= 2
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM recent
    WHERE unit_price > (SELECT MIN(unit_price) FROM recent)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        unit_price,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id, YEAR(moved_at) ORDER BY qty DESC, id) AS rn,
        COUNT(*) OVER (PARTITION BY warehouse_id) AS rows_in_warehouse_id,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_id, YEAR(moved_at) ORDER BY qty DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_2_prior,
        MAX(qty) OVER (PARTITION BY warehouse_id, YEAR(moved_at) ORDER BY qty DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS qty_max_4
    FROM filtered
),
joined_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.rn,
        s.rows_in_warehouse_id,
        s.qty_avg_2_prior,
        s.qty_max_4,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city
    FROM with_windows s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.city IN ('Bangkok', 'Kuala Lumpur')
    WHERE w.capacity <= 11005
)
SELECT *
FROM (
    SELECT warehouse_id, rn, warehouse_city, qty_avg_2_prior
    FROM joined_warehouse
) src
PIVOT (
    MIN(qty_avg_2_prior)
    FOR warehouse_city IN ('Singapore', 'Bangkok')
)
