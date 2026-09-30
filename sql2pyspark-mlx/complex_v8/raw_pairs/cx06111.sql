WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        voltage + kwh AS voltage_plus_kwh,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE voltage BETWEEN 221.8 AND 232.4
      AND quality IN ('BAD', 'EST')
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM cleaned s
    JOIN meter_readings m ON m.site_id = s.site_id
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM line_level_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at >= TIMESTAMP '2025-02-06 10:00:00'
        )
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY reading_quality, site_id) AS rows_in_reading_quality_site_id,
        LAG(reading_kwh) OVER (PARTITION BY reading_id, reading_quality ORDER BY reading_kwh ASC, id, reading_id) AS prev_reading_kwh,
        MIN(reading_kwh) OVER (PARTITION BY site_id, reading_id ORDER BY reading_kwh ASC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_reading_kwh_so_far,
        MIN(reading_kwh) OVER (PARTITION BY reading_quality, site_id) AS reading_quality_site_id_min_reading_kwh,
        COUNT(*) OVER (PARTITION BY site_id, reading_id ORDER BY reading_kwh ASC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(reading_kwh) OVER w_reading_quality_site_id, 2) AS reading_quality_site_id_avg_reading_kwh
    FROM screened
    WINDOW w_reading_quality_site_id AS (PARTITION BY reading_quality, site_id)
)
-- Subtotals with GROUPING SETS
SELECT
    reading_id,
    site_id,
    GROUPING_ID() AS grouping_level,
    SUM(running_count) AS total_running_count,
    ROUND(SUM(min_reading_kwh_so_far) / NULLIF(SUM(prev_reading_kwh), 0), 3) AS min_reading_kwh_so_far_to_prev_reading_kwh_ratio,
    COUNT(*) AS cnt
FROM with_windows
GROUP BY GROUPING SETS ((reading_id, site_id), (reading_id), (site_id), ())
