WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('WEEK', read_at) AS read_at_week
    FROM meter_readings
    WHERE quality NOT IN ('EST', 'OK')
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff,
        si.site_name
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id AND si.commissioned_on BETWEEN DATE '2024-06-27' AND DATE '2025-11-28'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        read_at_week,
        site_name,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY site_tariff, MONTH(read_at_week) ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        DENSE_RANK() OVER (PARTITION BY site_tariff, MONTH(read_at_week) ORDER BY kwh) AS kwh_dense_rank,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY site_tariff)) AS distinct_site_id_in_site_tariff,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality, site_tariff)) AS distinct_site_id_in_quality_site_tariff,
        MAX(voltage) OVER (PARTITION BY site_tariff) AS site_tariff_max_voltage,
        COUNT(*) OVER (PARTITION BY quality, site_tariff) AS rows_in_quality_site_tariff,
        COUNT(*) OVER w_site_tariff AS rows_in_site_tariff,
        ROUND(site_installed_kw - AVG(site_installed_kw) OVER (PARTITION BY site_tariff), 2) AS site_installed_kw_vs_site_tariff_avg,
        NTILE(10) OVER (PARTITION BY site_id ORDER BY voltage DESC, id) AS voltage_decile,
        MIN(voltage) OVER w_site_tariff AS site_tariff_min_voltage,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_tariff, MONTH(read_at_week) ORDER BY kwh), 3) AS kwh_percent_rank,
        ROUND(AVG(voltage) OVER (PARTITION BY quality, site_tariff), 2) AS quality_site_tariff_avg_voltage,
        LAG(kwh, 1, 0) OVER (PARTITION BY site_tariff, MONTH(read_at_week) ORDER BY kwh, id) AS prev_kwh,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_tariff), 2) AS kwh_vs_site_tariff_avg
    FROM joined
    WINDOW w_site_tariff AS (PARTITION BY site_tariff)
)
SELECT
    id,
    site_id,
    kwh,
    voltage,
    read_at_week,
    first_known_kwh,
    kwh_dense_rank,
    distinct_site_id_in_site_tariff,
    distinct_site_id_in_quality_site_tariff,
    site_tariff_max_voltage,
    rows_in_quality_site_tariff,
    rows_in_site_tariff,
    site_installed_kw_vs_site_tariff_avg,
    voltage_decile,
    site_tariff_min_voltage,
    kwh_percent_rank,
    quality_site_tariff_avg_voltage,
    kwh_vs_site_tariff_avg,
    DAYOFWEEK(read_at_week) AS read_at_week_dayofweek
FROM ranked
WHERE site_id >= 8
ORDER BY rows_in_site_tariff, distinct_site_id_in_site_tariff DESC
