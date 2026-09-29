WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at + INTERVAL 1 DAYS AS read_at_plus_1d,
        DATE_TRUNC('DAY', read_at) AS read_at_day,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE site_id <= 6
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(m.kwh), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_kwh,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM valid_readings s
),
per_group AS (
    -- Group-level totals
    SELECT
        site_id,
        ROUND(SUM(kwh), 2) AS total_kwh,
        MAX(kwh) AS max_kwh,
        ROUND(SUM(voltage) / NULLIF(SUM(kwh), 0), 3) AS voltage_to_kwh_ratio
    FROM summarized_rows
    GROUP BY site_id
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        total_kwh - voltage_to_kwh_ratio AS total_kwh_minus_voltage_to_kwh_ratio
    FROM per_group
    WHERE max_kwh < 90.0
)
-- Native SQL UNPIVOT (wide to long)
SELECT site_id, metric_name, value
FROM (
    SELECT site_id, max_kwh, total_kwh_minus_voltage_to_kwh_ratio, voltage_to_kwh_ratio
    FROM scored
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric_name IN (max_kwh AS max_kwh_v, total_kwh_minus_voltage_to_kwh_ratio AS total_kwh_minus_voltage_to_kwh_ratio_v, voltage_to_kwh_ratio AS voltage_to_kwh_ratio_v)
) unp
ORDER BY site_id, metric_name
