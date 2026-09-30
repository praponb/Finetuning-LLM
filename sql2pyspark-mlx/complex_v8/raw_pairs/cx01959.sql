WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE quality IN ('EST', 'BAD')
),
joined AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality_or_unknown,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw
    FROM base s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality_or_unknown,
        site_commissioned_on,
        site_tariff,
        site_installed_kw,
        SUM(site_installed_kw) OVER (PARTITION BY site_tariff, DATE_TRUNC('MONTH', read_at) ORDER BY site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_site_installed_kw,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY YEAR(site_commissioned_on) ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        MAX(kwh) OVER (PARTITION BY YEAR(site_commissioned_on), quality_or_unknown) AS site_commissioned_on_year_quality_or_unknown_max,
        SUM(site_installed_kw) OVER w_read_at_year AS read_at_year_sum_site_installed_kw,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY YEAR(read_at)), 0), 4) AS voltage_share_of_read_at_year,
        LAG(site_installed_kw, 2) OVER w_site_commissioned_on_year_by_2 AS prev2_site_installed_kw,
        MAX(voltage) OVER (PARTITION BY YEAR(site_commissioned_on), quality_or_unknown) AS site_commissioned_on_year_quality_or_unknown_max_2,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (ORDER BY voltage DESC, id))) / 60, 1) AS minutes_since_prev_read_at,
        LAG(voltage, 2) OVER w_quality_or_unknown_read_at_m AS prev2_voltage,
        NTILE(10) OVER w_all_by_voltage AS voltage_decile,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at)), 0), 4) AS site_installed_kw_share_of_read_at_month,
        voltage - LAG(voltage) OVER (PARTITION BY YEAR(site_commissioned_on) ORDER BY kwh, id) AS voltage_change,
        ROUND(AVG(kwh) OVER (PARTITION BY site_tariff, quality_or_unknown ORDER BY voltage, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_3_prior
    FROM joined
    WINDOW w_read_at_year AS (PARTITION BY YEAR(read_at)), w_site_commissioned_on_year_by_2 AS (PARTITION BY YEAR(site_commissioned_on) ORDER BY kwh, id), w_quality_or_unknown_read_at_m AS (PARTITION BY quality_or_unknown, DATE_TRUNC('MONTH', read_at) ORDER BY voltage, id), w_all_by_voltage AS (ORDER BY voltage DESC, id)
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    site_commissioned_on,
    site_installed_kw,
    running_site_installed_kw,
    second_read_at,
    read_at_year_sum_site_installed_kw,
    voltage_share_of_read_at_year,
    prev2_site_installed_kw,
    site_commissioned_on_year_quality_or_unknown_max_2,
    minutes_since_prev_read_at,
    voltage_decile,
    site_installed_kw_share_of_read_at_month,
    voltage_change
FROM with_windows
ORDER BY site_installed_kw
