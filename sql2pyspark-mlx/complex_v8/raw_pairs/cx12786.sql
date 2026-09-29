WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN voltage >= 236.8 THEN 'A' WHEN voltage >= 222.1 THEN 'B' ELSE 'C' END AS voltage_tier,
        HOUR(read_at) AS read_at_hour
    FROM meter_readings
    WHERE (site_id >= 6 OR quality NOT IN ('BAD', 'EST'))
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.read_at_hour,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality
    FROM filtered_readings s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.voltage < 236.8
),
site_id_stats AS (
    -- Median read_at_hour per site_id
    SELECT site_id, PERCENTILE_APPROX(read_at_hour, 0.5) AS median_read_at_hour
    FROM line_level
    GROUP BY site_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_read_at_hour,
        ROUND(s.read_at_hour - g.median_read_at_hour, 2) AS read_at_hour_minus_median
    FROM line_level s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.read_at_hour >= g.median_read_at_hour
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.read_at_hour,
        s.reading_id,
        s.reading_read_at,
        s.reading_voltage,
        s.reading_kwh,
        s.reading_quality,
        s.median_read_at_hour,
        s.read_at_hour_minus_median,
        si.installed_kw AS site_installed_kw
    FROM vs_group s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw >= 96.9
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at_hour, reading_quality, read_at_hour_minus_median
    FROM with_details
) src
PIVOT (
    SUM(read_at_hour_minus_median) AS total, MAX(read_at_hour_minus_median) AS max_v
    FOR reading_quality IN ('EST', 'OK', 'BAD')
)
ORDER BY read_at_hour
