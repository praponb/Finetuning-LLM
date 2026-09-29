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
        unit_price,
        LEAST(unit_price, qty) AS least_unit_price_qty,
        COALESCE(qty, 0) AS qty_filled,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM stock_moves
    WHERE operator = 'op_dew'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(qty_filled) OVER (PARTITION BY warehouse_id, operator ORDER BY least_unit_price_qty DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_filled_max_2_prior,
        LAG(unit_price_per_qty) OVER (PARTITION BY warehouse_id, operator ORDER BY least_unit_price_qty DESC, id) AS prev_unit_price_per_qty,
        ROUND(qty - AVG(qty) OVER (PARTITION BY operator), 2) AS qty_vs_operator_avg,
        SUM(qty) OVER (PARTITION BY warehouse_id, operator ORDER BY least_unit_price_qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        MAX(unit_price_per_qty) OVER w_operator AS operator_max_unit_price_per_qty,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY operator)) AS distinct_sku_id_in_operator
    FROM recent
    WINDOW w_operator AS (PARTITION BY operator)
),
joined AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.least_unit_price_qty,
        s.unit_price_per_qty,
        s.prev_unit_price_per_qty,
        s.qty_vs_operator_avg,
        s.running_qty,
        s.operator_max_unit_price_per_qty,
        s.distinct_sku_id_in_operator,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity
    FROM with_trends s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE w.capacity < 11768
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        YEAR(warehouse_opened_on) AS warehouse_opened_on_year,
        unit_price_per_qty + unit_price AS unit_price_per_qty_plus_unit_price
    FROM joined
    WHERE sku_id IS NOT NULL
)
SELECT *
FROM (
    SELECT warehouse_opened_on_year, move_type, unit_price_per_qty_plus_unit_price
    FROM enriched
) src
PIVOT (
    COUNT(unit_price_per_qty_plus_unit_price) AS count_v, MAX(unit_price_per_qty_plus_unit_price) AS max_v
    FOR move_type IN ('ADJUST', 'OUTBOUND')
)
