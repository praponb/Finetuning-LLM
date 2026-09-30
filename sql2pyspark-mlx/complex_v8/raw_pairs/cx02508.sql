WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        QUARTER(read_at) AS read_at_quarter,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-25 11:00:00'
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM recent
    WHERE kwh > (SELECT MIN(kwh) FROM recent)
),
site_id_stats AS (
    -- Avg read_at_quarter per site_id
    SELECT site_id, AVG(read_at_quarter) AS avg_read_at_quarter
    FROM without_matches
    GROUP BY site_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_read_at_quarter,
        ROUND(s.read_at_quarter - g.avg_read_at_quarter, 2) AS read_at_quarter_minus_avg
    FROM without_matches s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.read_at_quarter > g.avg_read_at_quarter
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM outliers s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.voltage_per_kwh,
        s.read_at_date,
        s.avg_read_at_quarter,
        s.read_at_quarter_minus_avg,
        si.zone AS site_zone
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone IS NOT NULL
),
screened AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM with_details s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.installed_kw > 66.5
              AND si.commissioned_on IS NOT NULL
        )
),
screened_2 AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh BETWEEN 43.1 AND 85.7
        )
      AND s.site_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    site_id,
    quality,
    voltage_per_kwh,
    avg_read_at_quarter,
    read_at_quarter_minus_avg
FROM screened_2
ORDER BY site_id DESC NULLS FIRST
