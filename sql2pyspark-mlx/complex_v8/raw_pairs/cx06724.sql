WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (quality = 'OK' OR kwh < 83.8)
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN m.quality = 'BAD' THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT COUNT(CASE WHEN m.quality IN ('OK', 'EST') THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count_2
    FROM recent_readings s
),
enriched AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.reading_quality_count,
        s.reading_count,
        s.reading_quality_count_2,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw
    FROM profiled s
    LEFT JOIN sites si ON si.id = s.site_id
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MAX(read_at) AS last_reading_read_at,
        MAX(read_at) AS last_reading_read_at_2,
        COUNT(*) AS reading_count_2
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at,
        m.last_reading_read_at_2,
        COALESCE(m.reading_count_2, 0) AS reading_count_2
    FROM enriched s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.site_id
)
SELECT id, measure, amount
FROM (
    SELECT id, reading_quality_count, reading_count
    FROM with_counts
) src
UNPIVOT INCLUDE NULLS (
    amount FOR measure IN (reading_quality_count, reading_count)
) unp
