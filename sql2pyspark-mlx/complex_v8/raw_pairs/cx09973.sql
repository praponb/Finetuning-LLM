WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE quality = 'OK'
),
site_id_stats AS (
    -- Max voltage per site_id
    SELECT site_id, MAX(voltage) AS max_voltage
    FROM cleaned_readings
    GROUP BY site_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_voltage,
        ROUND(s.voltage - g.max_voltage, 2) AS voltage_minus_max
    FROM cleaned_readings s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.voltage = g.max_voltage
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.max_voltage,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality
    FROM compared s
    JOIN meter_readings m ON m.site_id = s.site_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        voltage,
        quality,
        reading_id,
        reading_kwh,
        reading_quality,
        CONCAT(reading_quality, '#', CAST(reading_id AS STRING)) AS reading_quality_ref,
        LOWER(REGEXP_REPLACE(TRIM(reading_quality), ' ', '_')) AS reading_quality_slug
    FROM detail_rows
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(reading_kwh, 2) OVER (ORDER BY voltage DESC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reading_kwh,
        ROW_NUMBER() OVER (PARTITION BY quality ORDER BY voltage DESC, id, reading_id) AS seq_no,
        MIN(reading_kwh) OVER (PARTITION BY quality ORDER BY voltage DESC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_reading_kwh_so_far,
        LEAD(voltage, 1, 0) OVER w_quality_by_voltage AS next_voltage,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_quality_reading_id, 0), 4) AS voltage_share_of_quality_reading_id
    FROM tagged
    WINDOW w_quality_by_voltage AS (PARTITION BY quality ORDER BY voltage DESC, id, reading_id), w_quality_reading_id AS (PARTITION BY quality, reading_id)
)
-- Final projection
SELECT
    id,
    voltage,
    reading_id,
    reading_kwh,
    reading_quality,
    reading_quality_ref,
    reading_quality_slug,
    second_reading_kwh,
    seq_no,
    next_voltage,
    ABS(reading_kwh - 57.3) AS reading_kwh_distance
FROM windowed
ORDER BY reading_kwh_distance, voltage DESC NULLS FIRST
