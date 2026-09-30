WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 6 AND 12
      AND voltage <= 238.3
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.greatest_kwh_voltage,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM filtered s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.read_at < TIMESTAMP '2025-02-18 12:00:00'
),
quality_stats AS (
    -- Avg kwh per quality
    SELECT quality, AVG(kwh) AS avg_kwh
    FROM exploded_detail_readings
    GROUP BY quality
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_kwh, ROUND(s.kwh - g.avg_kwh, 2) AS kwh_minus_avg
    FROM exploded_detail_readings s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh > g.avg_kwh
),
enriched_reading AS (
    -- Attach reading attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.greatest_kwh_voltage,
        s.reading_id,
        s.reading_read_at,
        s.avg_kwh,
        s.kwh_minus_avg,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh_2,
        si.zone AS site_zone
    FROM vs_group s
    LEFT JOIN meter_readings m ON m.id = s.reading_id AND m.voltage BETWEEN 220.2 AND 239.6
    LEFT JOIN sites si ON si.id = s.site_id
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM enriched_reading
    WHERE kwh_minus_avg > (SELECT AVG(kwh_minus_avg) FROM enriched_reading)
),
site_id_stats AS (
    -- Avg kwh_minus_avg per site_id
    SELECT site_id, AVG(kwh_minus_avg) AS avg_kwh_minus_avg
    FROM qualified
    GROUP BY site_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_kwh_minus_avg,
        ROUND(s.kwh_minus_avg - g.avg_kwh_minus_avg, 2) AS kwh_minus_avg_minus_avg
    FROM qualified s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh_minus_avg > g.avg_kwh_minus_avg
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(read_at) AS read_at_year,
        site_zone,
        SUM(CASE WHEN site_zone = 'Z4' THEN reading_kwh_2 ELSE 0 END) AS reading_kwh_2_site_zone_z4,
        COUNT(*) FILTER (WHERE site_zone IS NOT NULL) AS n_site_zone_is_not_null,
        ROUND(SUM(kwh_minus_avg_minus_avg) / NULLIF(SUM(reading_voltage), 0), 3) AS kwh_minus_avg_minus_avg_to_reading_voltage_ratio
    FROM outliers
    WHERE reading_quality = 'OK'
    GROUP BY 1, 2
)
SELECT
    read_at_year,
    site_zone,
    reading_kwh_2_site_zone_z4,
    n_site_zone_is_not_null,
    kwh_minus_avg_minus_avg_to_reading_voltage_ratio
FROM per_group
ORDER BY reading_kwh_2_site_zone_z4 DESC, read_at_year, site_zone
LIMIT 20
