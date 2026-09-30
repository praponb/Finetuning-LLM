WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE quality IS NOT NULL
      AND kwh <= 113.0
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality
    FROM prepared_readings s
    JOIN meter_readings m ON m.site_id = s.site_id AND HOUR(m.read_at) BETWEEN 6 AND 11
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_children
    WHERE voltage > (SELECT AVG(voltage) FROM with_children)
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.reading_id,
        s.reading_voltage,
        s.reading_kwh,
        s.reading_quality,
        si.zone AS site_zone,
        si.site_name
    FROM without_matches s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone <> 'Z4'
),
rollup_base AS (
    -- Summarise per group
    SELECT
        site_zone,
        MAX(reading_voltage) AS max_reading_voltage,
        ARRAY_SORT(COLLECT_SET(reading_quality)) AS reading_quality_list,
        COUNT(DISTINCT reading_id) AS distinct_reading_ids
    FROM with_details
    GROUP BY site_zone
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE max_reading_voltage > (SELECT AVG(max_reading_voltage) FROM rollup_base)
)
SELECT
    site_zone,
    max_reading_voltage,
    reading_quality_list,
    distinct_reading_ids,
    SUBSTRING(TRIM(site_zone), 1, 4) AS site_zone_prefix
FROM filtered
WHERE distinct_reading_ids >= 5
ORDER BY max_reading_voltage
