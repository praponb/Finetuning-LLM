WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        LENGTH(TRIM(operator)) AS operator_len,
        ABS(qty - 216) AS qty_distance
    FROM stock_moves
    WHERE qty BETWEEN 138 AND 300
      AND warehouse_id >= 3
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM filtered s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM summarized_rows
    WHERE move_type NOT IN ('ADJUST', 'INBOUND')
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, qty_distance, move_count, operator_len, days_since_moved_at
    FROM tagged
) src
UNPIVOT (
    reading FOR measure IN (qty_distance AS m_qty_distance, move_count AS move_count_v, operator_len AS m_operator_len, days_since_moved_at AS m_days_since_moved_at)
) unp
