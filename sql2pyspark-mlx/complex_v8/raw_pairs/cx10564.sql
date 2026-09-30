WITH base AS (
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
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        LENGTH(TRIM(operator)) AS operator_len,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix
    FROM stock_moves
    WHERE sku_id >= 2
),
sku_id_stats AS (
    -- Avg qty per sku_id
    SELECT sku_id, AVG(qty) AS avg_qty
    FROM base
    GROUP BY sku_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_qty, ROUND(s.qty - g.avg_qty, 2) AS qty_minus_avg
    FROM base s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.qty > g.avg_qty
),
latest_per_key AS (
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
        move_type_or_unknown,
        operator_len,
        move_type_prefix,
        avg_qty,
        qty_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY operator_len DESC, id DESC) AS recency_rank
        FROM compared
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    sku_id,
    qty,
    operator,
    unit_price,
    move_type_or_unknown,
    operator_len,
    move_type_prefix,
    avg_qty,
    qty_minus_avg
FROM latest_per_key
ORDER BY operator_len DESC, qty_minus_avg
