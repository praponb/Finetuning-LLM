WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage < 233.8
      AND quality IN ('EST', 'OK')
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count_2
    FROM base s
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM profiled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage < 237.5
        )
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.reading_count,
        si.tariff AS site_tariff
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff NOT IN ('TOU', 'EV')
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(site_tariff, 'ALL') AS site_tariff,
    site_id,
    reading_count,
    ROUND(STDDEV(kwh), 2) AS stddev_kwh,
    ROUND(100 * COUNT(CASE WHEN quality IN ('OK', 'BAD') THEN 1 END) / COUNT(*), 1) AS pct_quality_in_ok_bad
FROM with_parent_site
GROUP BY ROLLUP(site_tariff, site_id, reading_count)
