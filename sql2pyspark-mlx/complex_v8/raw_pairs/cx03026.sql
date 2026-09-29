WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE quality = 'BAD'
      AND site_id >= 2
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality_ref,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id AND si.commissioned_on >= DATE '2024-08-24'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER (PARTITION BY site_zone), 0), 4) AS site_installed_kw_share_of_site_zone,
        LAG(kwh, 3) OVER (PARTITION BY site_zone ORDER BY kwh, id) AS prev3_kwh,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER (PARTITION BY site_tariff, site_zone), 0), 4) AS site_installed_kw_share_of_site_tariff_site_zone,
        LEAD(voltage) OVER (PARTITION BY site_tariff ORDER BY kwh, id) AS next_voltage,
        SUM(voltage) OVER w_site_tariff_site_zone AS site_tariff_site_zone_sum_voltage,
        FIRST_VALUE(voltage) OVER (PARTITION BY site_id ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage,
        ROUND(site_installed_kw - AVG(site_installed_kw) OVER (PARTITION BY site_tariff, site_zone), 2) AS site_installed_kw_vs_site_tariff_site_zone_avg,
        LEAD(voltage, 2) OVER (PARTITION BY site_tariff ORDER BY kwh, id) AS next2_voltage,
        MAX(site_installed_kw) OVER (PARTITION BY site_zone) AS site_zone_max_site_installed_kw,
        RANK() OVER w_site_id_read_at_month_by_vol AS voltage_rank,
        LAG(site_installed_kw) OVER (PARTITION BY site_id, site_zone ORDER BY read_at ASC, id) AS prev_site_installed_kw,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_tariff, YEAR(read_at)), 2) AS kwh_vs_site_tariff_read_at_year_avg,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY site_tariff ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        voltage - LAG(voltage) OVER (PARTITION BY site_id, site_zone ORDER BY read_at ASC, id) AS voltage_change,
        SUM(voltage) OVER (PARTITION BY site_zone) AS site_zone_sum_voltage
    FROM joined_site
    WINDOW w_site_tariff_site_zone AS (PARTITION BY site_tariff, site_zone), w_site_id_read_at_month_by_vol AS (PARTITION BY site_id, MONTH(read_at) ORDER BY voltage)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE voltage_rank = 1
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        kwh,
        quality_ref,
        site_zone,
        site_installed_kw_share_of_site_tariff_site_zone,
        next_voltage,
        site_tariff_site_zone_sum_voltage,
        first_voltage,
        site_installed_kw_vs_site_tariff_site_zone_avg,
        next2_voltage,
        site_zone_max_site_installed_kw,
        voltage_rank,
        kwh_vs_site_tariff_read_at_year_avg,
        second_read_at,
        ROUND(site_zone_sum_voltage / NULLIF(SUM(site_zone_sum_voltage) OVER (PARTITION BY MONTH(second_read_at)), 0), 4) AS site_zone_sum_voltage_share_of_second_read_at_mo,
        ROUND(voltage_change / NULLIF(SUM(voltage_change) OVER (PARTITION BY MONTH(second_read_at)), 0), 4) AS voltage_change_share_of_second_read_at_month,
        FIRST_VALUE(voltage) OVER (PARTITION BY MONTH(read_at) ORDER BY next2_voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage_2,
        prev_site_installed_kw - LAG(prev_site_installed_kw) OVER w_site_zone_by_site_installed AS prev_site_installed_kw_change,
        ROUND(prev_site_installed_kw / NULLIF(SUM(prev_site_installed_kw) OVER w_second_read_at_month, 0), 4) AS prev_site_installed_kw_share_of_second_read_at_m,
        ROUND(AVG(next_voltage) OVER (PARTITION BY MONTH(read_at) ORDER BY next2_voltage ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS next_voltage_avg_6_prior,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY MONTH(second_read_at)), 0), 4) AS voltage_share_of_second_read_at_month
    FROM leaders
    WINDOW w_site_zone_by_site_installed AS (PARTITION BY site_zone ORDER BY site_installed_kw_share_of_site_tariff_site_zone ASC, id), w_second_read_at_month AS (PARTITION BY MONTH(second_read_at))
)
SELECT
    id,
    kwh,
    site_zone,
    site_installed_kw_share_of_site_tariff_site_zone,
    next_voltage,
    site_tariff_site_zone_sum_voltage,
    site_installed_kw_vs_site_tariff_site_zone_avg,
    next2_voltage,
    site_zone_max_site_installed_kw,
    voltage_rank,
    kwh_vs_site_tariff_read_at_year_avg,
    site_zone_sum_voltage_share_of_second_read_at_mo,
    first_voltage_2,
    prev_site_installed_kw_change,
    prev_site_installed_kw_share_of_second_read_at_m,
    voltage_share_of_second_read_at_month,
    LOWER(REGEXP_REPLACE(TRIM(site_zone), ' ', '_')) AS site_zone_slug,
    CASE WHEN voltage_rank >= 7 THEN 'large' WHEN voltage_rank >= 4 THEN 'medium' ELSE 'small' END AS voltage_rank_tier
FROM windowed_2
ORDER BY site_tariff_site_zone_sum_voltage DESC
