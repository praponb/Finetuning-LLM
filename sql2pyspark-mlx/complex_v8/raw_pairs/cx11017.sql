WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price
    FROM stock_moves
),
annotated AS (
    -- Derive helper columns
    SELECT *, CONCAT(operator, '#', CAST(sku_id AS STRING)) AS operator_ref
    FROM valid
),
sku_id_stats AS (
    -- Avg qty per sku_id
    SELECT sku_id, AVG(qty) AS avg_qty
    FROM annotated
    GROUP BY sku_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_qty, ROUND(s.qty - g.avg_qty, 2) AS qty_minus_avg
    FROM annotated s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.qty > g.avg_qty
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    operator,
    avg_qty,
    qty_minus_avg,
    COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
FROM compared
ORDER BY warehouse_id, id
LIMIT 10
