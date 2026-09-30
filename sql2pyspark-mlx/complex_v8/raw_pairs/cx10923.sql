WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ABS(voltage - 232.7) AS voltage_distance
    FROM meter_readings
    WHERE quality = 'OK'
      AND voltage <= 235.8
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM recent
    WHERE voltage_distance > (SELECT AVG(voltage_distance) FROM recent)
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count, COUNT(*) AS reading_count_2
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_count_2, 0) AS reading_count_2
    FROM without_matches s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.voltage_distance,
        s.reading_count,
        s.reading_count_2,
        si.installed_kw AS site_installed_kw
    FROM profiled s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw < 386.6
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    reading_count,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT site_id) AS distinct_site_ids,
    MAX(read_at) AS last_read_at,
    ROUND(AVG(reading_count_2), 2) AS avg_reading_count_2
FROM with_parent_site
GROUP BY GROUPING SETS ((site_id, reading_count), (site_id), ())
