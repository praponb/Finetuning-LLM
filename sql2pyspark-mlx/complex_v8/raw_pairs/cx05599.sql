WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
quality_stats AS (
    -- Median voltage per quality
    SELECT quality, PERCENTILE_APPROX(voltage, 0.5) AS median_voltage
    FROM valid_readings
    GROUP BY quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_voltage,
        ROUND(s.voltage - g.median_voltage, 2) AS voltage_minus_median
    FROM valid_readings s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.voltage >= g.median_voltage
),
totals AS (
    -- Summarise per group
    SELECT
        site_id,
        quality,
        SUM(CASE WHEN quality IN ('BAD', 'EST') THEN kwh ELSE 0 END) AS kwh_quality_in_bad_est,
        MIN(voltage_minus_median) AS min_voltage_minus_median,
        COUNT(DISTINCT quality) AS distinct_qualitys
    FROM above_average
    WHERE site_id IS NOT NULL
    GROUP BY site_id, quality
    HAVING AVG(voltage_minus_median) > -49.0
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM totals s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality = 'BAD'
        )
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, si.site_name, si.commissioned_on AS site_commissioned_on
    FROM screened s
    LEFT JOIN sites si ON si.id = s.site_id AND si.zone NOT IN ('Z3', 'Z2')
)
SELECT
    quality,
    site_id,
    GROUPING(quality) AS is_total_quality,
    ROUND(SUM(kwh_quality_in_bad_est), 2) AS total_kwh_quality_in_bad_est
FROM enriched
GROUP BY GROUPING SETS ((quality, site_id), (quality))
