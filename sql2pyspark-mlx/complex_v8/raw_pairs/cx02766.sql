WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE MONTH(read_at) IN (1, 2)
      AND quality = 'OK'
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.quality_len,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality
    FROM prepared s
    JOIN meter_readings m ON m.site_id = s.site_id
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.quality_len,
        s.reading_id,
        s.reading_kwh,
        s.reading_voltage,
        s.reading_quality,
        si.commissioned_on AS site_commissioned_on,
        m.read_at AS reading_read_at
    FROM exploded_detail_readings s
    JOIN sites si ON si.id = s.site_id
    LEFT JOIN meter_readings m ON m.id = s.reading_id
    WHERE si.commissioned_on >= DATE '2024-09-15'
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        reading_quality,
        site_id,
        quality_len,
        MAX(reading_read_at) AS last_reading_read_at,
        MIN(quality_len) AS min_quality_len,
        SUM(quality_len) AS total_quality_len,
        ROUND(STDDEV(reading_kwh), 2) AS stddev_reading_kwh,
        ROUND(100 * COUNT(CASE WHEN quality IN ('EST', 'BAD') THEN 1 END) / COUNT(*), 1) AS pct_quality_in_est_bad,
        COUNT(*) AS n_rows
    FROM with_details_site
    WHERE YEAR(site_commissioned_on) = 2024
    GROUP BY reading_quality, site_id, quality_len
    HAVING MAX(voltage) > 218.2
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) FILTER (WHERE quality IN ('BAD', 'EST')) AS reading_quality_count,
        COUNT(*) AS reading_count,
        COUNT(*) AS reading_count_2
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_count_2, 0) AS reading_count_2
    FROM grouped s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_counts s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh >= 14.4
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT reading_quality, site_id, field, reading
FROM (
    SELECT reading_quality, site_id, pct_quality_in_est_bad, stddev_reading_kwh
    FROM qualified
) src
UNPIVOT (
    reading FOR field IN (pct_quality_in_est_bad, stddev_reading_kwh)
) unp
ORDER BY reading_quality, site_id, field
