WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-02-06 08:00:00'
      AND quality = 'BAD'
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        si.commissioned_on AS site_commissioned_on
    FROM scoped s
    JOIN sites si ON si.id = s.site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT ROUND(AVG(m.voltage), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_voltage
    FROM joined s
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN reading_count >= 6 THEN 'large' WHEN reading_count >= 3 THEN 'medium' ELSE 'small' END AS reading_count_tier
    FROM with_counts
    WHERE site_id IS NOT NULL
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.reading_count,
        s.avg_reading_voltage,
        s.reading_count_tier,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh
    FROM derived s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.quality IN ('OK', 'BAD')
)
-- Final projection
SELECT
    id,
    site_id,
    voltage,
    reading_count,
    avg_reading_voltage,
    reading_count_tier,
    reading_id,
    reading_quality,
    reading_read_at,
    reading_read_at + INTERVAL 7 DAYS AS reading_read_at_plus_7d,
    LEAST(avg_reading_voltage, voltage) AS least_avg_reading_voltage_voltage
FROM line_level_readings
ORDER BY site_id DESC, reading_count_tier DESC NULLS FIRST
