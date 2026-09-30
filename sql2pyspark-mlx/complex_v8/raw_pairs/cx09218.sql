WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        voltage + kwh AS voltage_plus_kwh
    FROM meter_readings
    WHERE quality IN ('BAD', 'OK')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage_plus_kwh,
        SUM(voltage_plus_kwh) OVER (PARTITION BY site_id) AS site_id_sum_voltage_plus_kwh,
        ROUND(voltage_plus_kwh / NULLIF(SUM(voltage_plus_kwh) OVER (PARTITION BY site_id, quality), 0), 4) AS voltage_plus_kwh_share_of_site_id_quality,
        ROUND(CUME_DIST() OVER w_site_id_by_voltage_ties, 3) AS voltage_cume_dist,
        ROUND(AVG(voltage) OVER (PARTITION BY quality, site_id ORDER BY voltage_plus_kwh ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_3_prior,
        SIZE(COLLECT_SET(quality) OVER w_site_id) AS distinct_quality_in_site_id,
        FIRST_VALUE(voltage_plus_kwh) IGNORE NULLS OVER (PARTITION BY quality, YEAR(read_at) ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage_plus_kwh,
        ROUND(voltage_plus_kwh / NULLIF(SUM(voltage_plus_kwh) OVER (PARTITION BY site_id), 0), 4) AS voltage_plus_kwh_share_of_site_id,
        COUNT(*) OVER (PARTITION BY site_id, quality) AS rows_in_site_id_quality
    FROM base_readings
    WINDOW w_site_id_by_voltage_ties AS (PARTITION BY site_id ORDER BY voltage DESC), w_site_id AS (PARTITION BY site_id)
)
SELECT
    id,
    kwh,
    site_id_sum_voltage_plus_kwh,
    voltage_plus_kwh_share_of_site_id_quality,
    voltage_avg_3_prior,
    distinct_quality_in_site_id,
    voltage_plus_kwh_share_of_site_id,
    rows_in_site_id_quality
FROM with_windows
ORDER BY distinct_quality_in_site_id DESC, id DESC
