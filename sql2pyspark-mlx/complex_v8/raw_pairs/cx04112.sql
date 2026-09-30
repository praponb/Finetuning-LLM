WITH recent_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        CASE WHEN installed_kw >= 373.3 THEN 'HIGH' WHEN installed_kw >= 96.9 THEN 'MEDIUM' ELSE 'LOW' END AS installed_kw_tier,
        CASE WHEN installed_kw >= 386.6 THEN 'large' WHEN installed_kw >= 141.6 THEN 'medium' ELSE 'small' END AS installed_kw_tier_2
    FROM sites
    WHERE LENGTH(TRIM(site_name)) >= 4
      AND zone IS NOT NULL
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE m.quality IN ('OK', 'EST')) FROM meter_readings m WHERE m.site_id = s.id) AS reading_quality_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.id) AS reading_count,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.id) AS first_reading_read_at
    FROM recent_sites s
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.kwh BETWEEN 35.0 AND 117.1
        )
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.id) AS reading_count_2
    FROM qualified s
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(tariff, 'ALL') AS tariff,
    COALESCE(zone, 'ALL') AS zone,
    ROUND(AVG(installed_kw), 2) AS avg_installed_kw,
    PERCENTILE_APPROX(reading_count, 0.5) AS median_reading_count,
    MIN(first_reading_read_at) AS first_first_reading_read_at
FROM profiled
GROUP BY GROUPING SETS ((tariff, zone), (tariff), (zone), ())
ORDER BY tariff NULLS LAST, zone NULLS LAST
