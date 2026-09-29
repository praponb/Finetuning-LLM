WITH scoped_plays AS (
    -- Filter plays
    SELECT *
    FROM plays
    WHERE is_skipped = TRUE
      AND ms_played >= 73335
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        is_skipped,
        PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played,
        COUNT(*) FILTER (WHERE device IN ('car', 'phone')) AS n_device_in_car_phone,
        SUM(ms_played) AS total_ms_played,
        SUM(CASE WHEN device <> 'car' THEN 1 ELSE 0 END) AS n_device_car
    FROM scoped_plays
    GROUP BY DATE_TRUNC('MONTH', played_at), is_skipped
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE n_device_in_car_phone > (SELECT AVG(n_device_in_car_phone) FROM rollup_base)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('DAY', played_at_month) AS played_at_month_day,
        TO_DATE(played_at_month) AS played_at_month_date
    FROM without_matches
    WHERE n_device_car > 3
)
SELECT
    played_at_month,
    median_ms_played,
    n_device_in_car_phone,
    n_device_car,
    played_at_month_day,
    played_at_month_date,
    TRUNC(played_at_month_date, 'MM') AS played_at_month_date_month_start,
    ROUND(n_device_car / NULLIF(n_device_in_car_phone, 0), 3) AS n_device_car_per_n_device_in_car_phone
FROM tagged
ORDER BY n_device_car_per_n_device_in_car_phone DESC, n_device_in_car_phone DESC
