WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        ABS(unit_price - 272.4) AS unit_price_distance,
        CONCAT(operator, '#', CAST(id AS STRING)) AS operator_ref
    FROM stock_moves
    WHERE qty > 99
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, w.city AS warehouse_city, w.opened_on AS warehouse_opened_on
    FROM valid_moves s
    JOIN warehouses w ON w.id = s.warehouse_id AND LOWER(w.wh_code) LIKE '%cn%'
    WHERE w.opened_on IS NOT NULL
),
enriched_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.unit_price_distance,
        s.operator_ref,
        s.warehouse_city,
        s.warehouse_opened_on,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity
    FROM with_parent s
    JOIN warehouses w ON w.id = s.warehouse_id
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', warehouse_opened_on) AS warehouse_opened_on_month,
        sku_id,
        SUM(unit_price) FILTER (WHERE warehouse_city = 'Bangkok') AS unit_price_warehouse_city_bangkok,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        ARRAY_SORT(COLLECT_SET(warehouse_wh_code)) AS warehouse_wh_code_list,
        DATEDIFF(MAX(warehouse_opened_on), MIN(warehouse_opened_on)) AS warehouse_opened_on_span_days
    FROM enriched_warehouse
    WHERE qty >= 111
    GROUP BY 1, 2
    HAVING MAX(unit_price) > 124.1
)
SELECT
    warehouse_opened_on_month,
    unit_price_warehouse_city_bangkok,
    move_type_list,
    warehouse_wh_code_list,
    ARRAY_JOIN(ARRAY_SORT(warehouse_wh_code_list), ',') AS warehouse_wh_code_list_text,
    SIZE(warehouse_wh_code_list) AS warehouse_wh_code_list_count
FROM per_group
ORDER BY warehouse_wh_code_list_text, unit_price_warehouse_city_bangkok NULLS LAST
