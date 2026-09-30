WITH prepared AS (
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
        CASE WHEN operator IN ('op_ann', 'op_bee') THEN 'group_a' WHEN operator = 'op_eve' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
    WHERE unit_price <= 396.4
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(moved_at) OVER (PARTITION BY move_type, sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at,
        NTILE(3) OVER (PARTITION BY operator_group, operator ORDER BY unit_price ASC, id) AS unit_price_tercile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator_group, operator ORDER BY unit_price ASC), 3) AS unit_price_percent_rank,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY TO_DATE(moved_at), warehouse_id), 2) AS unit_price_vs_moved_at_to_date_warehouse_id_avg,
        MIN(unit_price) OVER w_moved_at_to_date_warehouse_i AS moved_at_to_date_warehouse_id_min_unit_price,
        qty - LAG(qty) OVER w_sku_id_by_unit_price AS qty_change
    FROM prepared
    WINDOW w_moved_at_to_date_warehouse_i AS (PARTITION BY TO_DATE(moved_at), warehouse_id), w_sku_id_by_unit_price AS (PARTITION BY sku_id ORDER BY unit_price DESC, id)
),
sku_id_stats AS (
    -- Avg unit_price_vs_moved_at_to_date_warehouse_id_avg per sku_id
    SELECT sku_id, AVG(unit_price_vs_moved_at_to_date_warehouse_id_avg) AS avg_unit_price_vs_moved_at_to_date_warehouse_id_
    FROM with_trends
    GROUP BY sku_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_unit_price_vs_moved_at_to_date_warehouse_id_,
        ROUND(s.unit_price_vs_moved_at_to_date_warehouse_id_avg - g.avg_unit_price_vs_moved_at_to_date_warehouse_id_, 2) AS unit_price_vs_moved_at_to_date_warehouse_id_avg_
    FROM with_trends s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.unit_price_vs_moved_at_to_date_warehouse_id_avg > g.avg_unit_price_vs_moved_at_to_date_warehouse_id_
)
SELECT id, metric, value
FROM (
    SELECT id, qty, qty_change
    FROM vs_group
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (qty, qty_change)
) unp
