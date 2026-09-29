WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage + kwh AS voltage_plus_kwh,
        DATE_TRUNC('YEAR', read_at) AS read_at_year,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM scoped s
    JOIN sites si ON si.id = s.site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS min_reading_voltage,
        (SELECT ROUND(AVG(m.voltage), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_voltage,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM combined_details s
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        site_tariff,
        site_zone,
        min_reading_voltage,
        avg_reading_voltage,
        reading_count,
        DATE_TRUNC('YEAR', read_at) AS read_at_year_2
    FROM summarized_rows
)
SELECT
    reading_count,
    quality,
    site_id,
    ARRAY_SORT(COLLECT_SET(site_tariff)) AS site_tariff_list,
    SUM(CASE WHEN quality IS NOT NULL THEN min_reading_voltage ELSE 0 END) AS min_reading_voltage_quality_is_not_null,
    COUNT(*) AS n_rows
FROM scored
GROUP BY GROUPING SETS ((reading_count, quality, site_id), (reading_count))
