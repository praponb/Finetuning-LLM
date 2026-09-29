WITH scoped AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        QUARTER(commissioned_on) AS commissioned_on_quarter,
        CASE WHEN installed_kw >= 373.3 THEN 'A' WHEN installed_kw >= 96.9 THEN 'B' ELSE 'C' END AS installed_kw_tier
    FROM sites
    WHERE zone = 'Z3'
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        COUNT(CASE WHEN quality = 'OK' THEN 1 END) AS reading_quality_count,
        MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        m.first_reading_read_at
    FROM scoped s
    LEFT JOIN reading_stats m ON m.site_id = s.id
)
SELECT *
FROM (
    SELECT commissioned_on_quarter, zone, reading_quality_count
    FROM with_counts
) src
PIVOT (
    MAX(reading_quality_count) AS max_v, SUM(reading_quality_count) AS sum_v
    FOR zone IN ('Z3', 'Z2', 'Z4')
)
ORDER BY commissioned_on_quarter
