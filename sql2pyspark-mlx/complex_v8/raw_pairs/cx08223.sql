WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage - kwh AS voltage_minus_kwh,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE voltage < 227.6
      AND TO_DATE(read_at) < DATE '2025-03-08'
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ABS(voltage - 221.4) AS voltage_distance,
        CASE WHEN quality IN ('BAD', 'OK') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM base_readings
    WHERE kwh > 35.9
),
with_details_site AS (
    -- Join sites
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on
    FROM enriched s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw <= 284.0
),
unique_rows AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage_minus_kwh,
        kwh_per_voltage,
        voltage_distance,
        quality_group,
        site_installed_kw,
        site_commissioned_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh_per_voltage DESC, id DESC) AS recency_rank
        FROM with_details_site
    ) ranked
    WHERE recency_rank = 1
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM unique_rows s
)
SELECT
    id,
    site_id,
    quality,
    voltage_minus_kwh,
    kwh_per_voltage,
    site_installed_kw,
    site_commissioned_on,
    first_reading_read_at,
    LEAST(voltage_distance, voltage) AS least_voltage_distance_voltage,
    DATEDIFF(DATE '2025-12-31', first_reading_read_at) AS days_since_first_reading_read_at
FROM with_counts
WHERE quality NOT IN ('OK', 'EST')
ORDER BY site_id DESC, site_commissioned_on DESC
