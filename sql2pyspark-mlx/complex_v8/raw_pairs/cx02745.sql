WITH scoped AS (
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
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE qty BETWEEN 123 AND 394
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.warehouse_id >= 5
        )
      AND s.operator = 'op_ann'
),
deduped AS (
    -- Keep the latest row per warehouse_id
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
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY qty DESC, id DESC) AS recency_rank
        FROM without_matches
    ) ranked
    WHERE recency_rank = 1
),
warehouse_id_stats AS (
    -- Avg days_since_moved_at per warehouse_id
    SELECT warehouse_id, AVG(days_since_moved_at) AS avg_days_since_moved_at
    FROM deduped
    GROUP BY warehouse_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_moved_at,
        ROUND(s.days_since_moved_at - g.avg_days_since_moved_at, 2) AS days_since_moved_at_minus_avg
    FROM deduped s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.days_since_moved_at > g.avg_days_since_moved_at
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        days_since_moved_at,
        avg_days_since_moved_at,
        LEAST(unit_price, avg_days_since_moved_at) AS least_unit_price_avg_days_since_moved_at
    FROM outliers
    WHERE days_since_moved_at < 560
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    avg_days_since_moved_at,
    least_unit_price_avg_days_since_moved_at
FROM scored
ORDER BY move_type, least_unit_price_avg_days_since_moved_at
