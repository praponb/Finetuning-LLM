WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
    FROM meter_readings
    WHERE kwh <= 78.6
      AND site_id <= 6
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.site_name,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM base_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY quality), 2) AS voltage_vs_quality_avg,
        RANK() OVER w_all_by_kwh AS kwh_rank,
        ROUND(voltage_per_kwh - AVG(voltage_per_kwh) OVER (PARTITION BY site_zone, site_id), 2) AS voltage_per_kwh_vs_site_zone_site_id_avg,
        RANK() OVER w_site_id_by_read_at_ties AS read_at_rank,
        SIZE(COLLECT_SET(site_id) OVER w_quality) AS distinct_site_id_in_quality,
        voltage - LAG(voltage) OVER (ORDER BY kwh, id) AS voltage_change,
        FIRST_VALUE(site_installed_kw) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_site_installed_kw,
        SUM(voltage) OVER (PARTITION BY quality, site_zone ORDER BY voltage, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS voltage_sum_6_prior,
        ROUND(PERCENT_RANK() OVER w_site_id_site_zone_by_read_at, 3) AS read_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY read_at), 3) AS read_at_cume_dist,
        MAX(site_installed_kw) OVER (ORDER BY kwh, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS site_installed_kw_max_2,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY site_id, site_zone ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        MAX(voltage) OVER (PARTITION BY site_zone, DATE_TRUNC('MONTH', read_at) ORDER BY voltage_per_kwh DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_max_2_prior
    FROM combined_details_site
    WINDOW w_all_by_kwh AS (ORDER BY kwh, id), w_site_id_by_read_at_ties AS (PARTITION BY site_id ORDER BY read_at), w_quality AS (PARTITION BY quality), w_site_id_site_zone_by_read_at AS (PARTITION BY site_id, site_zone ORDER BY read_at DESC)
)
SELECT
    id,
    site_id,
    kwh,
    voltage,
    quality,
    voltage_per_kwh,
    site_installed_kw,
    voltage_vs_quality_avg,
    voltage_per_kwh_vs_site_zone_site_id_avg,
    read_at_rank,
    distinct_site_id_in_quality,
    voltage_change,
    voltage_sum_6_prior,
    read_at_percent_rank,
    site_installed_kw_max_2,
    second_voltage
FROM sequenced
WHERE quality = 'EST'
ORDER BY id, site_id NULLS LAST
