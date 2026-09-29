WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        commissioned_on,
        QUARTER(commissioned_on) AS commissioned_on_quarter
    FROM sites
    WHERE site_name IS NOT NULL
      AND zone IN ('Z4', 'Z2')
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MAX(voltage) AS max_reading_voltage, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.max_reading_voltage,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM valid s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_counts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.voltage >= 215.7
        )
)
SELECT
    id,
    zone,
    commissioned_on,
    max_reading_voltage,
    reading_count,
    CASE WHEN reading_count >= 7 THEN 'HIGH' WHEN reading_count >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS reading_count_tier,
    reading_count - commissioned_on_quarter AS reading_count_minus_commissioned_on_quarter
FROM without_matches
ORDER BY reading_count_minus_commissioned_on_quarter NULLS LAST, id
