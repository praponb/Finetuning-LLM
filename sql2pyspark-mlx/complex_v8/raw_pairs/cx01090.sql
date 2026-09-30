WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        ABS(kwh - 50.4) AS kwh_distance,
        LOWER(TRIM(quality)) AS quality_clean,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM meter_readings
    WHERE kwh <= 101.2
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        site_id,
        COUNT(*) AS n_rows,
        MAX(kwh_distance) AS max_kwh_distance,
        ROUND(SUM(voltage) / NULLIF(SUM(greatest_kwh_voltage), 0), 3) AS voltage_to_greatest_kwh_voltage_ratio,
        MIN(greatest_kwh_voltage) AS min_greatest_kwh_voltage,
        COUNT(*) FILTER (WHERE quality IS NOT NULL) AS n_quality_is_not_null,
        MIN(kwh_distance) AS min_kwh_distance
    FROM prepared_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE m.quality NOT IN ('BAD', 'EST')) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count
    FROM totals s
)
-- Final projection
SELECT
    site_id,
    n_rows,
    max_kwh_distance,
    voltage_to_greatest_kwh_voltage_ratio,
    min_greatest_kwh_voltage,
    n_quality_is_not_null,
    min_kwh_distance,
    reading_quality_count,
    LEAST(n_rows, n_quality_is_not_null) AS least_n_rows_n_quality_is_not_null,
    ROUND(reading_quality_count / NULLIF(max_kwh_distance, 0), 3) AS reading_quality_count_per_max_kwh_distance
FROM profiled
ORDER BY min_kwh_distance DESC NULLS FIRST, least_n_rows_n_quality_is_not_null
