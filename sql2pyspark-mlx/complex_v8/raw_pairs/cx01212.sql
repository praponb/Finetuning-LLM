WITH base_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE qty >= 169
      AND sku_id <= 3
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.operator = 'op_ann') FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_operator_count
    FROM base_moves s
),
sku_id_stats AS (
    -- Avg unit_price per sku_id
    SELECT sku_id, AVG(unit_price) AS avg_unit_price
    FROM profiled
    GROUP BY sku_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_unit_price,
        ROUND(s.unit_price - g.avg_unit_price, 2) AS unit_price_minus_avg
    FROM profiled s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.unit_price > g.avg_unit_price
)
SELECT id, metric_name, value
FROM (
    SELECT id, unit_price_minus_avg, avg_unit_price
    FROM outliers
) src
UNPIVOT (
    value FOR metric_name IN (unit_price_minus_avg, avg_unit_price)
) unp
