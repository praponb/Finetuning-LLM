WITH base AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        SUBSTRING(TRIM(zone), 1, 4) AS zone_prefix
    FROM sites
    WHERE tariff NOT IN ('EV', 'FLAT')
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.id) AS last_reading_read_at
    FROM base s
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        MAX(read_at) AS last_reading_read_at_2
    FROM meter_readings
    GROUP BY site_id
),
with_stats_2 AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.last_reading_read_at_2
    FROM with_stats s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.id
)
SELECT *
FROM (
    SELECT tariff, zone, reading_count
    FROM with_stats_2
) src
PIVOT (
    SUM(reading_count)
    FOR zone IN ('Z3', 'Z4', 'Z2')
)
ORDER BY tariff
