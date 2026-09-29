WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        FLOOR(capacity / 5000) * 5000 AS capacity_bucket,
        DAYOFWEEK(opened_on) AS opened_on_dayofweek,
        DATEDIFF(DATE '2025-06-30', opened_on) AS days_since_opened_on
    FROM warehouses
    WHERE capacity >= 2614
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        MAX(moved_at) AS last_moved_at,
        MAX(qty) AS max_move_qty,
        COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at,
        st.max_move_qty,
        COALESCE(st.move_count, 0) AS move_count
    FROM cleaned s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(last_moved_at, 'yyyy-MM') AS last_moved_at_ym,
        opened_on_dayofweek,
        MAX(last_moved_at) AS last_last_moved_at,
        DATEDIFF(MAX(opened_on), MIN(opened_on)) AS opened_on_span_days,
        COUNT(*) FILTER (WHERE city = 'Kuala Lumpur') AS n_city_kuala_lumpur,
        ROUND(STDDEV(days_since_opened_on), 2) AS stddev_days_since_opened_on,
        ROUND(SUM(capacity) / NULLIF(SUM(capacity_bucket), 0), 3) AS capacity_to_capacity_bucket_ratio
    FROM summarized_rows
    GROUP BY DATE_FORMAT(last_moved_at, 'yyyy-MM'), opened_on_dayofweek
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        last_moved_at_ym,
        opened_on_dayofweek,
        last_last_moved_at,
        SUM(opened_on_dayofweek) OVER (ORDER BY stddev_days_since_opened_on, last_moved_at_ym, opened_on_dayofweek ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS opened_on_dayofweek_sum_1_prior,
        SUM(stddev_days_since_opened_on) OVER (PARTITION BY n_city_kuala_lumpur ORDER BY capacity_to_capacity_bucket_ratio ASC, last_moved_at_ym, opened_on_dayofweek ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS stddev_days_since_opened_on_sum_6_prior,
        NTILE(4) OVER (ORDER BY stddev_days_since_opened_on, last_moved_at_ym, opened_on_dayofweek) AS stddev_days_since_opened_on_quartile,
        ROUND(CUME_DIST() OVER w_n_city_kuala_lumpur_by_capac_2, 3) AS capacity_to_capacity_bucket_ratio_cume_dist
    FROM per_group
    WINDOW w_n_city_kuala_lumpur_by_capac_2 AS (PARTITION BY n_city_kuala_lumpur ORDER BY capacity_to_capacity_bucket_ratio ASC)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE stddev_days_since_opened_on_quartile > (SELECT AVG(stddev_days_since_opened_on_quartile) FROM metrics)
)
SELECT
    last_moved_at_ym,
    opened_on_dayofweek,
    opened_on_dayofweek_sum_1_prior,
    stddev_days_since_opened_on_sum_6_prior,
    stddev_days_since_opened_on_quartile,
    capacity_to_capacity_bucket_ratio_cume_dist,
    UPPER(TRIM(last_moved_at_ym)) AS last_moved_at_ym_clean,
    CASE WHEN opened_on_dayofweek >= 6 THEN 'HIGH' WHEN opened_on_dayofweek >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS opened_on_dayofweek_tier
FROM with_activity
ORDER BY last_moved_at_ym NULLS LAST, opened_on_dayofweek_sum_1_prior DESC
