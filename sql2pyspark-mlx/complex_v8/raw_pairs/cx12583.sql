WITH valid_moves AS (
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
        ROUND(unit_price * 0.9, 2) AS unit_price_adj,
        UPPER(TRIM(operator)) AS operator_clean,
        CASE WHEN unit_price >= 292.3 THEN 'HIGH' WHEN unit_price >= 180.0 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 7 AND 10
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        sku_id,
        TO_DATE(moved_at) AS moved_at_day,
        COUNT(DISTINCT operator) AS distinct_operators,
        ROUND(SUM(unit_price_adj), 2) AS total_unit_price_adj,
        ROUND(100 * COUNT(CASE WHEN operator IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_operator_is_not_null
    FROM valid_moves
    GROUP BY sku_id, TO_DATE(moved_at)
),
with_details AS (
    -- Enrich with dimension data
    SELECT s.*, sk.labels AS sku_labels
    FROM rollup_base s
    LEFT JOIN skus sk ON sk.id = s.sku_id
)
-- Final projection
SELECT
    sku_id,
    moved_at_day,
    distinct_operators,
    sku_labels,
    ROUND(total_unit_price_adj / NULLIF(distinct_operators, 0), 3) AS total_unit_price_adj_per_distinct_operators,
    ADD_MONTHS(moved_at_day, 6) AS moved_at_day_plus_6m
FROM with_details
WHERE distinct_operators < 8
ORDER BY distinct_operators DESC
