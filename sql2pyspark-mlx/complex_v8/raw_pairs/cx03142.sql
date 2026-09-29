WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE kwh > 35.9
      AND site_id <= 5
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.days_since_read_at,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at
    FROM valid s
    JOIN meter_readings m ON m.site_id = s.site_id
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.reading_id,
        s.reading_read_at,
        m.id AS reading_id_2,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality_2,
        m.voltage AS reading_voltage_2
    FROM with_children_readings s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.kwh >= 26.9
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM with_children
    WHERE reading_voltage_2 > (SELECT MIN(reading_voltage_2) FROM with_children)
)
-- Subtotals with ROLLUP
SELECT
    reading_id_2,
    COALESCE(reading_quality_2, 'ALL') AS reading_quality_2,
    GROUPING_ID() AS gid,
    MIN(reading_kwh) AS min_reading_kwh,
    COUNT(DISTINCT reading_id_2) AS distinct_reading_id_2s
FROM filtered
GROUP BY ROLLUP(reading_id_2, reading_quality_2)
ORDER BY reading_id_2 NULLS LAST, reading_quality_2 NULLS LAST
