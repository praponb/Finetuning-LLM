WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LEAST(voltage, kwh) AS least_voltage_kwh
    FROM meter_readings
    WHERE quality NOT IN ('OK', 'EST')
      AND HOUR(read_at) BETWEEN 9 AND 15
),
with_parent AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM base s
    LEFT JOIN sites si ON si.id = s.site_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(voltage - AVG(voltage) OVER w_quality, 2) AS voltage_vs_quality_avg,
        FIRST_VALUE(voltage) OVER (PARTITION BY quality, site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER (PARTITION BY quality), 0), 4) AS site_installed_kw_share_of_quality,
        MIN(voltage) OVER (PARTITION BY site_id, site_tariff ORDER BY site_installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_so_far,
        MAX(site_installed_kw) OVER (PARTITION BY site_id, site_tariff ORDER BY site_installed_kw ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS site_installed_kw_max_7,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY site_id, site_tariff ORDER BY site_installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        ROUND(CUME_DIST() OVER (ORDER BY kwh DESC, id), 3) AS kwh_cume_dist,
        MAX(voltage) OVER (PARTITION BY quality, site_id ORDER BY kwh, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS voltage_max_3_prior,
        COUNT(*) OVER (PARTITION BY site_id, site_tariff ORDER BY site_installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(voltage) OVER w_quality AS quality_sum_voltage,
        LAST_VALUE(site_installed_kw) OVER (PARTITION BY site_id, site_tariff ORDER BY site_installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_site_installed_kw,
        SUM(site_installed_kw) OVER (PARTITION BY site_id, site_tariff ORDER BY site_installed_kw ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS site_installed_kw_sum_4
    FROM with_parent
    WINDOW w_quality AS (PARTITION BY quality)
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        quality,
        site_name,
        voltage_vs_quality_avg,
        first_voltage,
        site_installed_kw_share_of_quality,
        min_voltage_so_far,
        site_installed_kw_max_7,
        first_known_kwh,
        kwh_cume_dist,
        running_count,
        quality_sum_voltage,
        last_site_installed_kw,
        ROUND(AVG(kwh_cume_dist) OVER (PARTITION BY site_id, quality), 2) AS site_id_quality_avg_kwh_cume_dist,
        LEAD(kwh) OVER (ORDER BY site_installed_kw_share_of_quality NULLS LAST, id) AS next_kwh,
        SIZE(COLLECT_SET(site_tariff) OVER w_site_id_quality) AS distinct_site_tariff_in_site_id_quality
    FROM sequenced
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality)
)
SELECT
    id,
    site_id,
    quality,
    voltage_vs_quality_avg,
    min_voltage_so_far,
    site_installed_kw_max_7,
    running_count,
    site_id_quality_avg_kwh_cume_dist,
    next_kwh,
    LOWER(REGEXP_REPLACE(TRIM(site_name), ' ', '_')) AS site_name_slug,
    COALESCE(first_voltage, 0.0) AS first_voltage_filled
FROM sequenced_2
ORDER BY quality NULLS LAST, id
LIMIT 20
