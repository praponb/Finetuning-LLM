WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        FLOOR(voltage / 5) * 5 AS voltage_bucket
    FROM meter_readings
    WHERE kwh <= 80.8
      AND site_id >= 4
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT SUM(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_voltage,
        (SELECT MAX(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_voltage,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM cleaned_readings s
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.voltage_bucket,
        s.max_reading_voltage,
        s.reading_count,
        si.commissioned_on AS site_commissioned_on,
        si.site_name
    FROM with_stats s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        kwh,
        quality,
        max_reading_voltage,
        reading_count,
        site_commissioned_on,
        ROUND(AVG(kwh) OVER (PARTITION BY quality), 2) AS quality_avg_kwh,
        COUNT(*) OVER w_quality AS rows_in_quality,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality), 0), 4) AS kwh_share_of_quality,
        SUM(reading_count) OVER (PARTITION BY reading_count ORDER BY max_reading_voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_reading_count,
        SIZE(COLLECT_SET(site_id) OVER w_quality) AS distinct_site_id_in_quality,
        SUM(reading_count) OVER (PARTITION BY quality) AS quality_sum_reading_count
    FROM with_details
    WINDOW w_quality AS (PARTITION BY quality)
)
SELECT
    id,
    kwh,
    max_reading_voltage,
    reading_count,
    quality_avg_kwh,
    rows_in_quality,
    running_reading_count,
    distinct_site_id_in_quality,
    COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
FROM with_windows
WHERE reading_count <= 6
ORDER BY max_reading_voltage DESC, rows_in_quality DESC NULLS FIRST
