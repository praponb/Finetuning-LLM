WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        LEAST(unit_price, qty) AS least_unit_price_qty,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND unit_price > 60.4
),
with_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.least_unit_price_qty,
        w.opened_on AS warehouse_opened_on
    FROM cleaned s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
deduped AS (
    -- Keep the latest row per operator, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        least_unit_price_qty,
        warehouse_opened_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, move_type ORDER BY warehouse_opened_on DESC, id DESC) AS rn
        FROM with_details_warehouse
    ) ranked
    WHERE rn = 1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        move_type,
        operator,
        least_unit_price_qty,
        warehouse_opened_on,
        ROUND(AVG(least_unit_price_qty) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at)), 2) AS moved_at_month_avg_least_unit_price_qty,
        ROUND(least_unit_price_qty - AVG(least_unit_price_qty) OVER w_moved_at_month, 2) AS least_unit_price_qty_vs_moved_at_month_avg,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        LEAD(moved_at, 3) OVER (ORDER BY moved_at DESC, id) AS next3_moved_at,
        DATEDIFF(warehouse_opened_on, LAG(warehouse_opened_on) OVER (ORDER BY moved_at DESC, id)) AS days_since_prev_warehouse_opened_on,
        MAX(least_unit_price_qty) OVER w_moved_at_month AS moved_at_month_max_least_unit_price_qty
    FROM deduped
    WINDOW w_moved_at_month AS (PARTITION BY DATE_TRUNC('MONTH', moved_at))
),
enriched AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.least_unit_price_qty_vs_moved_at_month_avg,
        s.first_known_moved_at,
        s.next3_moved_at,
        s.days_since_prev_warehouse_opened_on,
        s.moved_at_month_max_least_unit_price_qty,
        sk.category AS sku_category,
        sk.is_active AS sku_is_active,
        sk.unit_cost AS sku_unit_cost
    FROM metrics s
    JOIN skus sk ON sk.id = s.sku_id AND sk.is_active = FALSE
    WHERE sk.unit_cost <= 330.6
),
deduped_2 AS (
    -- Keep the latest row per sku_id
    SELECT
        id,
        sku_id,
        move_type,
        operator,
        least_unit_price_qty_vs_moved_at_month_avg,
        first_known_moved_at,
        next3_moved_at,
        days_since_prev_warehouse_opened_on,
        moved_at_month_max_least_unit_price_qty,
        sku_category,
        sku_is_active,
        sku_unit_cost
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY days_since_prev_warehouse_opened_on DESC, id DESC) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
joined_sku AS (
    -- Enrich with dimension data
    SELECT s.*, sk.unit_cost AS sku_unit_cost_2
    FROM deduped_2 s
    JOIN skus sk ON sk.id = s.sku_id AND sk.unit_cost <= 277.0
)
SELECT id, measure, amount
FROM (
    SELECT id, sku_unit_cost_2, sku_unit_cost
    FROM joined_sku
) src
UNPIVOT (
    amount FOR measure IN (sku_unit_cost_2 AS sku_unit_cost_2_v, sku_unit_cost AS sku_unit_cost_v)
) unp
