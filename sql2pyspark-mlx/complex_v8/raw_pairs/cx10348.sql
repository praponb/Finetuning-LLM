WITH base_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        SUBSTRING(TRIM(operator), 1, 4) AS operator_prefix
    FROM stock_moves
    WHERE sku_id >= 12
      AND moved_at >= TIMESTAMP '2025-01-25 07:00:00'
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM base_moves s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
unique_rows AS (
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
        operator_prefix,
        move_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY unit_price DESC, id DESC) AS recency_rank
        FROM summarized_rows
    ) ranked
    WHERE recency_rank = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT move_count, moved_at, move_type, unit_price
    FROM unique_rows
) src
PIVOT (
    SUM(unit_price) AS amount, COUNT(unit_price) AS n
    FOR move_type IN ('INBOUND', 'OUTBOUND', 'TRANSFER')
)
