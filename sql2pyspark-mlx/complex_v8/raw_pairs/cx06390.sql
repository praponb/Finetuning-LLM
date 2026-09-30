WITH scoped_moves AS (
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
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE operator IS NOT NULL
      AND moved_at < TIMESTAMP '2025-03-08 09:00:00'
),
deduped AS (
    -- Keep the latest row per move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        days_since_moved_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY moved_at DESC, id DESC) AS row_num
        FROM scoped_moves
    ) ranked
    WHERE row_num = 1
),
warehouse_id_stats AS (
    -- Median unit_price per warehouse_id
    SELECT warehouse_id, PERCENTILE_APPROX(unit_price, 0.5) AS median_unit_price
    FROM deduped
    GROUP BY warehouse_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_unit_price,
        ROUND(s.unit_price - g.median_unit_price, 2) AS unit_price_minus_median
    FROM deduped s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.unit_price >= g.median_unit_price
)
SELECT id, field, reading
FROM (
    SELECT id, days_since_moved_at, qty
    FROM vs_group
) src
UNPIVOT (
    reading FOR field IN (days_since_moved_at AS m_days_since_moved_at, qty AS m_qty)
) unp
