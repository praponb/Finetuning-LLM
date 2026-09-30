WITH prepared AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        LENGTH(TRIM(city)) AS city_len,
        DATE_TRUNC('MONTH', opened_on) AS opened_on_month,
        CONCAT_WS('-', city, wh_code) AS city_wh_code_label
    FROM warehouses
    WHERE wh_code IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        wh_code,
        city,
        opened_on,
        city_len,
        opened_on_month,
        city_wh_code_label,
        FIRST_VALUE(opened_on) OVER (ORDER BY opened_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_on,
        ROUND(capacity - AVG(capacity) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on_month), city), 2) AS capacity_vs_opened_on_month_month_city_avg,
        SUM(capacity) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on_month), city) AS opened_on_month_month_city_sum_capacity,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on_month), city) AS rows_in_opened_on_month_month_city,
        SUM(city_len) OVER (ORDER BY opened_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS city_len_sum_3
    FROM prepared
),
city_stats AS (
    -- Median opened_on_month_month_city_sum_capacity per city
    SELECT city, PERCENTILE_APPROX(opened_on_month_month_city_sum_capacity, 0.5) AS median_opened_on_month_month_city_sum_capacity
    FROM windowed
    GROUP BY city
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_opened_on_month_month_city_sum_capacity,
        ROUND(s.opened_on_month_month_city_sum_capacity - g.median_opened_on_month_month_city_sum_capacity, 2) AS opened_on_month_month_city_sum_capacity_minus_me
    FROM windowed s
    JOIN city_stats g ON g.city = s.city
    WHERE s.opened_on_month_month_city_sum_capacity >= g.median_opened_on_month_month_city_sum_capacity
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.id) AS last_moved_at,
        (SELECT MAX(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.id) AS max_move_qty
    FROM outliers s
)
SELECT id, metric, reading
FROM (
    SELECT id, city_len, rows_in_opened_on_month_month_city, max_move_qty
    FROM summarized_rows
) src
UNPIVOT (
    reading FOR metric IN (city_len, rows_in_opened_on_month_month_city, max_move_qty)
) unp
