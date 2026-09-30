WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE MONTH(read_at) IN (2, 3)
),
stats AS (
    -- Group-level totals
    SELECT
        TO_DATE(read_at) AS read_at_day,
        site_id,
        MIN(voltage) AS min_voltage,
        ROUND(STDDEV(voltage), 2) AS stddev_voltage,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        SUM(kwh) FILTER (WHERE quality IN ('BAD', 'EST')) AS kwh_quality_in_bad_est,
        SUM(days_since_read_at) FILTER (WHERE quality NOT IN ('BAD', 'OK')) AS days_since_read_at_quality_not_in_bad_ok
    FROM valid_readings
    GROUP BY TO_DATE(read_at), site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at,
        (SELECT ROUND(AVG(m.voltage), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_voltage
    FROM stats s
)
-- Final projection
SELECT
    read_at_day,
    min_voltage,
    stddev_voltage,
    quality_list,
    days_since_read_at_quality_not_in_bad_ok,
    avg_reading_voltage
FROM with_stats
ORDER BY read_at_day DESC, days_since_read_at_quality_not_in_bad_ok DESC
