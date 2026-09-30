WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality), 0), 4) AS kwh_share_of_quality,
        kwh - LAG(kwh) OVER (PARTITION BY site_id, MONTH(read_at) ORDER BY kwh ASC, id) AS kwh_change,
        FIRST_VALUE(read_at) OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        MAX(kwh) OVER (PARTITION BY quality) AS quality_max_kwh,
        COUNT(*) OVER w_quality AS rows_in_quality
    FROM prepared_readings
    WINDOW w_quality AS (PARTITION BY quality)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, kwh, quality_max_kwh, kwh_share_of_quality
    FROM with_windows
) src
UNPIVOT (
    metric_value FOR metric_name IN (kwh, quality_max_kwh, kwh_share_of_quality)
) unp
