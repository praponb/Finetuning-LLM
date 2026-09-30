WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        ROUND(unit_price * 1.1, 2) AS unit_price_adj
    FROM stock_moves
    WHERE qty >= 198
      AND unit_price >= 160.8
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        unit_price,
        unit_price_adj,
        LOWER(TRIM(operator)) AS operator_clean,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix,
        CASE WHEN operator IN ('op_ann', 'op_dew') THEN 'group_a' WHEN operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM recent
),
warehouse_id_stats AS (
    -- Avg unit_price_adj per warehouse_id
    SELECT warehouse_id, AVG(unit_price_adj) AS avg_unit_price_adj
    FROM enriched
    GROUP BY warehouse_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_unit_price_adj,
        ROUND(s.unit_price_adj - g.avg_unit_price_adj, 2) AS unit_price_adj_minus_avg
    FROM enriched s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.unit_price_adj > g.avg_unit_price_adj
)
SELECT
    sku_id,
    warehouse_id,
    COALESCE(operator_group, 'ALL') AS operator_group,
    ROUND(STDDEV(unit_price_adj_minus_avg), 2) AS stddev_unit_price_adj_minus_avg,
    COUNT(DISTINCT operator_group) AS distinct_operator_groups
FROM outliers
GROUP BY CUBE(sku_id, warehouse_id, operator_group)
ORDER BY sku_id NULLS LAST, warehouse_id NULLS LAST, operator_group NULLS LAST
