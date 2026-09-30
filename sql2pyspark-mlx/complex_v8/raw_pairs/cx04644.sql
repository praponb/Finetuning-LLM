WITH scoped_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        city,
        capacity,
        opened_on,
        DATEDIFF(DATE '2025-06-30', opened_on) AS days_since_opened_on,
        MONTH(opened_on) AS opened_on_month
    FROM warehouses
    WHERE wh_code IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(opened_on) IGNORE NULLS OVER (PARTITION BY city ORDER BY opened_on_month ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_on,
        ROW_NUMBER() OVER w_city_by_opened_on_month AS seq_no,
        MIN(capacity) OVER (PARTITION BY opened_on_month, city) AS opened_on_month_city_min_capacity,
        ROUND(AVG(days_since_opened_on) OVER (PARTITION BY city ORDER BY opened_on_month ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS days_since_opened_on_avg_2,
        NTH_VALUE(days_since_opened_on, 2) OVER (PARTITION BY city ORDER BY opened_on_month ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_opened_on
    FROM scoped_warehouses
    WINDOW w_city_by_opened_on_month AS (PARTITION BY city ORDER BY opened_on_month ASC, id)
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_count_2,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.id) AS first_moved_at
    FROM sequenced s
)
SELECT
    id,
    city,
    capacity,
    days_since_opened_on,
    first_known_opened_on,
    seq_no,
    opened_on_month_city_min_capacity,
    days_since_opened_on_avg_2,
    second_days_since_opened_on,
    move_count_2
FROM with_stats
ORDER BY move_count_2 NULLS LAST
