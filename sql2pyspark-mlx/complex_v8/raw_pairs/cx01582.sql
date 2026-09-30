WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        voltage + kwh AS voltage_plus_kwh
    FROM meter_readings
    WHERE site_id >= 6
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(voltage_per_kwh) OVER (PARTITION BY site_id, quality), 2) AS site_id_quality_avg_voltage_per_kwh,
        ROUND(voltage_per_kwh - AVG(voltage_per_kwh) OVER (PARTITION BY site_id, quality), 2) AS voltage_per_kwh_vs_site_id_quality_avg,
        LEAD(read_at, 3) OVER w_site_id_by_voltage AS next3_read_at,
        LAST_VALUE(voltage_plus_kwh) OVER (PARTITION BY quality ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage_plus_kwh
    FROM valid_readings
    WINDOW w_site_id_by_voltage AS (PARTITION BY site_id ORDER BY voltage DESC, id)
),
latest_per_key AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage_per_kwh,
        voltage_plus_kwh,
        site_id_quality_avg_voltage_per_kwh,
        voltage_per_kwh_vs_site_id_quality_avg,
        next3_read_at,
        last_voltage_plus_kwh
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh DESC, id DESC) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM latest_per_key s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality = 'BAD'
        )
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.voltage_plus_kwh,
        s.site_id_quality_avg_voltage_per_kwh,
        s.voltage_per_kwh_vs_site_id_quality_avg,
        s.last_voltage_plus_kwh,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM qualified s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_parent_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.voltage_plus_kwh,
        s.site_id_quality_avg_voltage_per_kwh,
        s.voltage_per_kwh_vs_site_id_quality_avg,
        s.site_installed_kw,
        si.installed_kw AS site_installed_kw_2,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on_2
    FROM with_details s
    JOIN sites si ON si.id = s.site_id
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_parent_site s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND MONTH(m.read_at) IN (1, 3)
              AND m.voltage >= 222.6
        )
)
SELECT
    id,
    site_id,
    read_at,
    quality,
    site_id_quality_avg_voltage_per_kwh,
    site_installed_kw,
    site_tariff,
    site_commissioned_on_2
FROM screened
WHERE site_installed_kw < 284.0
ORDER BY site_installed_kw NULLS LAST, site_commissioned_on_2
