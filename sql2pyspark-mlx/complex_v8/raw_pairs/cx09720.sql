WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        LEAST(kwh, voltage) AS least_kwh_voltage,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
    FROM meter_readings
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, si.tariff AS site_tariff, si.commissioned_on AS site_commissioned_on
    FROM scoped s
    LEFT JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        least_kwh_voltage,
        quality_prefix,
        site_tariff,
        site_commissioned_on,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), site_id)) AS distinct_site_tariff_in_site_commissioned_on_mon,
        ROUND(AVG(least_kwh_voltage) OVER (PARTITION BY site_id, quality ORDER BY least_kwh_voltage, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS least_kwh_voltage_avg_6_prior,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY site_tariff, TO_DATE(read_at) ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        voltage - LAG(voltage) OVER w_all_by_read_at AS voltage_change,
        COUNT(*) OVER w_site_id_quality AS rows_in_site_id_quality,
        voltage - LAG(voltage) OVER (ORDER BY site_commissioned_on DESC, id) AS voltage_change_2,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), site_id)) AS distinct_quality_in_site_commissioned_on_month_s,
        ROUND(AVG(voltage) OVER (PARTITION BY site_tariff, TO_DATE(read_at) ORDER BY voltage DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_3_prior
    FROM enriched
    WINDOW w_all_by_read_at AS (ORDER BY read_at, id), w_site_id_quality AS (PARTITION BY site_id, quality)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('-', quality_prefix, quality) AS quality_prefix_quality_label,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(last_known_read_at)) / 3600, 1) AS hours_last_known_read_at_to_read_at
    FROM windowed
    WHERE read_at >= TIMESTAMP '2025-01-19 12:00:00'
)
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality,
    least_kwh_voltage,
    quality_prefix,
    site_commissioned_on,
    least_kwh_voltage_avg_6_prior,
    last_known_read_at,
    voltage_change,
    rows_in_site_id_quality,
    distinct_quality_in_site_commissioned_on_month_s,
    voltage_avg_3_prior,
    quality_prefix_quality_label
FROM labeled
ORDER BY voltage_avg_3_prior NULLS LAST
