WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh > 20.4
      AND voltage <= 240.2
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        kwh,
        HOUR(read_at) AS read_at_hour
    FROM prepared
    WHERE voltage <= 232.4
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        SUM(read_at_hour) OVER (PARTITION BY site_id) AS site_id_sum_read_at_hour
    FROM tagged
    WINDOW w_site_id AS (PARTITION BY site_id)
)
-- Subtotals with ROLLUP
SELECT
    site_id,
    read_at_hour,
    PERCENTILE_APPROX(site_id_sum_read_at_hour, 0.5) AS median_site_id_sum_read_at_hour,
    ROUND(AVG(site_id_sum_read_at_hour), 2) AS avg_site_id_sum_read_at_hour
FROM with_windows
GROUP BY ROLLUP(site_id, read_at_hour)
ORDER BY site_id NULLS LAST, read_at_hour NULLS LAST
