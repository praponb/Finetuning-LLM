WITH recent_moves AS (
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
        FLOOR(qty / 100) * 100 AS qty_bucket,
        ABS(qty - 148) AS qty_distance,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-13 12:00:00'
),
annotated AS (
    -- Derive helper columns
    SELECT *, DATE_TRUNC('YEAR', moved_at) AS moved_at_year
    FROM recent_moves
    WHERE operator IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(qty) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price
    FROM annotated
),
joined_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.qty_bucket,
        s.qty_distance,
        s.operator_or_unknown,
        s.moved_at_year,
        s.last_known_unit_price,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity
    FROM ranked s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
operator_or_unknown_stats AS (
    -- Median last_known_unit_price per operator_or_unknown
    SELECT operator_or_unknown, PERCENTILE_APPROX(last_known_unit_price, 0.5) AS median_last_known_unit_price
    FROM joined_warehouse
    GROUP BY operator_or_unknown
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_last_known_unit_price,
        ROUND(s.last_known_unit_price - g.median_last_known_unit_price, 2) AS last_known_unit_price_minus_median
    FROM joined_warehouse s
    JOIN operator_or_unknown_stats g ON g.operator_or_unknown = s.operator_or_unknown
    WHERE s.last_known_unit_price >= g.median_last_known_unit_price
)
SELECT
    id,
    warehouse_id,
    move_type,
    moved_at,
    operator,
    unit_price,
    qty_bucket,
    qty_distance,
    operator_or_unknown,
    warehouse_opened_on,
    warehouse_capacity,
    median_last_known_unit_price,
    DATEDIFF(DATE '2025-12-31', warehouse_opened_on) AS days_since_warehouse_opened_on,
    LOWER(TRIM(operator_or_unknown)) AS operator_or_unknown_clean
FROM compared
WHERE last_known_unit_price > 111.7
ORDER BY qty_distance, qty_bucket
