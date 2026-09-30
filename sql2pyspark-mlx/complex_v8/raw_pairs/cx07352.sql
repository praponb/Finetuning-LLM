WITH scoped AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        commissioned_on,
        DATE_TRUNC('YEAR', commissioned_on) AS commissioned_on_year
    FROM sites
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.quality IS NOT NULL
        )
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        MIN(read_at) AS first_reading_read_at,
        COUNT(*) AS reading_count_2
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.first_reading_read_at,
        COALESCE(m.reading_count_2, 0) AS reading_count_2
    FROM screened s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.kwh >= 50.4
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, reading
FROM (
    SELECT id, reading_count, reading_count_2
    FROM with_activity
) src
UNPIVOT (
    reading FOR metric IN (reading_count AS reading_count_v, reading_count_2 AS reading_count_2_v)
) unp
ORDER BY id, metric
