WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        ROUND(voltage * 1.1, 2) AS voltage_adj
    FROM meter_readings
    WHERE kwh >= 42.1
      AND voltage >= 216.8
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.voltage_adj,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage
    FROM valid_readings s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
-- Subtotals with ROLLUP
SELECT
    reading_id,
    quality,
    GROUPING_ID() AS grouping_level,
    SUM(CASE WHEN quality IS NOT NULL THEN 1 ELSE 0 END) AS n_quality_is_not_null,
    COUNT(*) AS cnt
FROM detail_rows
GROUP BY ROLLUP(reading_id, quality)
ORDER BY reading_id NULLS LAST, quality NULLS LAST
