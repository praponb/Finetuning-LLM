WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        voltage - kwh AS voltage_minus_kwh,
        ROUND(kwh * 1.07, 2) AS kwh_adj,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-02 13:00:00'
),
enriched_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage_minus_kwh,
        s.quality_ref,
        si.tariff AS site_tariff
    FROM base s
    JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(voltage_minus_kwh / NULLIF(SUM(voltage_minus_kwh) OVER (PARTITION BY YEAR(read_at)), 0), 4) AS voltage_minus_kwh_share_of_read_at_year,
        ROUND(voltage_minus_kwh - AVG(voltage_minus_kwh) OVER (PARTITION BY YEAR(read_at)), 2) AS voltage_minus_kwh_vs_read_at_year_avg
    FROM enriched_site
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.quality_ref,
        s.site_tariff,
        s.voltage_minus_kwh_vs_read_at_year_avg,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage
    FROM with_windows s
    JOIN meter_readings m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    quality_ref,
    site_tariff,
    voltage_minus_kwh_vs_read_at_year_avg,
    reading_id,
    reading_quality,
    reading_kwh,
    reading_read_at,
    reading_voltage
FROM detail_rows
WHERE reading_kwh BETWEEN 30.2 AND 85.7
