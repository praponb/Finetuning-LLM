WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE (moved_at >= TIMESTAMP '2025-02-12 10:00:00' OR qty >= 91)
),
annotated AS (
    -- Label and score rows
    SELECT *, LENGTH(TRIM(operator)) AS operator_len
    FROM base_moves
),
warehouse_id_stats AS (
    -- Avg qty per warehouse_id
    SELECT warehouse_id, AVG(qty) AS avg_qty
    FROM annotated
    GROUP BY warehouse_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_qty, ROUND(s.qty - g.avg_qty, 2) AS qty_minus_avg
    FROM annotated s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.qty > g.avg_qty
),
joined AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.unit_price,
        s.operator_len,
        s.avg_qty,
        s.qty_minus_avg,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city
    FROM above_average s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE w.city IS NOT NULL
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        operator_len,
        avg_qty,
        warehouse_wh_code,
        warehouse_city,
        COALESCE(warehouse_city, 'UNKNOWN') AS warehouse_city_or_unknown
    FROM joined
),
unique_rows AS (
    -- Keep the latest row per warehouse_id, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        operator_len,
        avg_qty,
        warehouse_wh_code,
        warehouse_city,
        warehouse_city_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id, move_type ORDER BY avg_qty DESC, id DESC) AS row_num
        FROM derived
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    operator_len,
    avg_qty,
    warehouse_wh_code,
    warehouse_city,
    warehouse_city_or_unknown,
    INITCAP(TRIM(warehouse_wh_code)) AS warehouse_wh_code_clean
FROM unique_rows
ORDER BY id
