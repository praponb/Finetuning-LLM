WITH joined_ranges AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.quality,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        COUNT(*) AS reading_count_2,
        MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_count_2, 0) AS reading_count_2,
        m.last_reading_read_at
    FROM joined_ranges s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.zone,
        s.rate,
        s.energy_cost,
        s.reading_count,
        s.last_reading_read_at,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality
    FROM summarized_rows s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.quality NOT IN ('OK', 'BAD')
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        rate,
        reading_count,
        reading_id,
        reading_kwh,
        reading_quality,
        GREATEST(reading_voltage, reading_kwh) AS greatest_reading_voltage_reading_kwh,
        CASE WHEN zone IN ('Z4', 'Z3') THEN 'group_a' WHEN zone = 'Z2' THEN 'group_b' ELSE 'other' END AS zone_group,
        HOUR(last_reading_read_at) AS last_reading_read_at_hour
    FROM line_level_readings
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT last_reading_read_at_hour, reading_count, zone_group, greatest_reading_voltage_reading_kwh
    FROM derived
) src
PIVOT (
    COUNT(greatest_reading_voltage_reading_kwh)
    FOR zone_group IN ('group_b', 'other', 'group_a')
)
