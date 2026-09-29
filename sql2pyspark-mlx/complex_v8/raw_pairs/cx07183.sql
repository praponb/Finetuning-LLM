WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (read_at < TIMESTAMP '2025-02-24 11:00:00' OR voltage >= 223.1)
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) FILTER (WHERE quality = 'BAD') AS reading_quality_count,
        COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM cleaned_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.reading_count,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM with_counts s
    JOIN meter_readings m ON m.site_id = s.site_id
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM detail_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh <= 111.9
              AND m.quality <> 'EST'
        )
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE m.quality = 'EST') FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count_2,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM without_matches s
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', reading_read_at) AS days_since_reading_read_at,
        CASE WHEN reading_quality_count >= 4 THEN 'A' WHEN reading_quality_count >= 2 THEN 'B' ELSE 'C' END AS reading_quality_count_tier
    FROM with_stats
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.reading_count,
        s.reading_id,
        s.reading_read_at,
        s.reading_quality_count,
        s.last_reading_read_at,
        s.reading_quality_count_tier,
        m.id AS reading_id_2,
        m.read_at AS reading_read_at_2,
        m.kwh AS reading_kwh_2,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality_2
    FROM scored s
    JOIN meter_readings m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    voltage,
    reading_count,
    reading_id,
    reading_read_at,
    reading_quality_count,
    reading_quality_count_tier,
    reading_read_at_2,
    reading_kwh_2,
    reading_voltage,
    reading_quality_2,
    ROUND(reading_voltage * 1.5, 2) AS reading_voltage_adj,
    CASE WHEN reading_quality_2 IN ('EST', 'OK') THEN 'group_a' WHEN reading_quality_2 = 'BAD' THEN 'group_b' ELSE 'other' END AS reading_quality_2_group
FROM with_children
ORDER BY id DESC, reading_read_at_2
