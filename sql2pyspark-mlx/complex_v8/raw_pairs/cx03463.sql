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
        CASE WHEN move_type IN ('INBOUND', 'OUTBOUND') THEN 'group_a' WHEN move_type = 'ADJUST' THEN 'group_b' ELSE 'other' END AS move_type_group,
        ABS(unit_price - 292.3) AS unit_price_distance
    FROM stock_moves
    WHERE move_type = 'ADJUST'
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(move_type_group, '#', CAST(qty AS STRING)) AS move_type_group_ref,
        ABS(qty - 332) AS qty_distance
    FROM valid_moves
),
combined_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_group,
        s.unit_price_distance,
        s.qty_distance,
        w.capacity AS warehouse_capacity
    FROM scored s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.city IN ('Kuala Lumpur', 'Bangkok')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        qty_distance - LAG(qty_distance) OVER w_sku_id_by_qty_distance AS qty_distance_change,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY YEAR(moved_at), warehouse_id)) AS distinct_operator_in_moved_at_year_warehouse_id
    FROM combined_details_warehouse
    WINDOW w_sku_id_by_qty_distance AS (PARTITION BY sku_id ORDER BY qty_distance DESC, id)
),
combined_details_warehouse_2 AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.move_type_group,
        s.unit_price_distance,
        s.qty_distance,
        s.warehouse_capacity,
        s.qty_distance_change,
        s.distinct_operator_in_moved_at_year_warehouse_id,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity_2
    FROM with_windows s
    JOIN warehouses w ON w.id = s.warehouse_id
),
warehouse_city_stats AS (
    -- Max warehouse_capacity per warehouse_city
    SELECT warehouse_city, MAX(warehouse_capacity) AS max_warehouse_capacity
    FROM combined_details_warehouse_2
    GROUP BY warehouse_city
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_warehouse_capacity,
        ROUND(s.warehouse_capacity - g.max_warehouse_capacity, 2) AS warehouse_capacity_minus_max
    FROM combined_details_warehouse_2 s
    JOIN warehouse_city_stats g ON g.warehouse_city = s.warehouse_city
    WHERE s.warehouse_capacity = g.max_warehouse_capacity
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        warehouse_city,
        TO_DATE(moved_at) AS moved_at_day,
        ROUND(100 * COUNT(CASE WHEN warehouse_city = 'Singapore' THEN 1 END) / COUNT(*), 1) AS pct_warehouse_city_singapore,
        COUNT(*) AS cnt
    FROM outliers
    WHERE max_warehouse_capacity <= 15250.0
    GROUP BY warehouse_city, TO_DATE(moved_at)
)
SELECT warehouse_city, pct_warehouse_city_singapore, cnt
FROM grouped
