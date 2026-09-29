WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        CASE WHEN capacity >= 11506 THEN 'HIGH' WHEN capacity >= 4912 THEN 'MEDIUM' ELSE 'LOW' END AS capacity_tier,
        CASE WHEN city IN ('Bangkok', 'Singapore') THEN 'group_a' WHEN city = 'Kuala Lumpur' THEN 'group_b' ELSE 'other' END AS city_group,
        CONCAT_WS(': ', wh_code, city) AS wh_code_city_label
    FROM warehouses
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) AS move_count,
        MIN(qty) AS min_move_qty,
        SUM(unit_price) AS total_move_unit_price
    FROM stock_moves
    GROUP BY warehouse_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        st.min_move_qty,
        st.total_move_unit_price
    FROM base s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
        )
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM with_activity
    WHERE min_move_qty > (SELECT MIN(min_move_qty) FROM with_activity)
),
with_activity_2 AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM screened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND TO_DATE(st.moved_at) < DATE '2025-02-24'
              AND st.sku_id IS NOT NULL
        )
      AND YEAR(s.opened_on) = 2025
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(move_count) OVER (ORDER BY total_move_unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_move_count,
        LAG(capacity, 3) OVER (ORDER BY total_move_unit_price ASC, id) AS prev3_capacity,
        SUM(capacity) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on) ORDER BY total_move_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_capacity,
        NTH_VALUE(total_move_unit_price, 2) OVER (ORDER BY total_move_unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_move_unit_price,
        MIN(min_move_qty) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on) ORDER BY total_move_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_min_move_qty_so_far
    FROM with_activity_2
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        wh_code_city_label,
        move_count,
        total_move_unit_price,
        running_move_count,
        prev3_capacity,
        running_capacity,
        second_total_move_unit_price,
        move_count + capacity AS move_count_plus_capacity
    FROM with_windows
)
-- Final projection
SELECT
    id,
    wh_code,
    city,
    capacity,
    opened_on,
    move_count,
    total_move_unit_price,
    running_move_count,
    prev3_capacity,
    move_count_plus_capacity,
    DAYOFWEEK(opened_on) AS opened_on_dayofweek
FROM enriched
ORDER BY capacity DESC, city
