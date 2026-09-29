WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ROUND(kwh * 0.25, 2) AS kwh_adj,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE voltage BETWEEN 220.2 AND 241.9
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.quality_slug,
        si.tariff AS site_tariff,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM filtered_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff = 'TOU'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        voltage,
        quality,
        site_tariff,
        site_zone,
        site_installed_kw,
        LAG(site_installed_kw, 3) OVER w_site_tariff_read_at_month_by AS prev3_site_installed_kw,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER w_site_zone, 0), 4) AS site_installed_kw_share_of_site_zone,
        MIN(site_installed_kw) OVER (ORDER BY site_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_site_installed_kw_so_far,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY site_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        ROW_NUMBER() OVER (ORDER BY read_at, id) AS read_at_seq,
        ROUND(AVG(site_installed_kw) OVER (ORDER BY site_installed_kw, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS site_installed_kw_avg_3_prior,
        NTILE(5) OVER w_all_by_read_at AS read_at_quintile,
        MAX(voltage) OVER (PARTITION BY quality, site_id) AS quality_site_id_max_voltage,
        NTILE(4) OVER w_site_tariff_read_at_month_by AS read_at_quartile,
        SUM(site_installed_kw) OVER (PARTITION BY site_tariff) AS site_tariff_sum_site_installed_kw,
        ROUND(AVG(voltage) OVER (PARTITION BY quality, site_id), 2) AS quality_site_id_avg_voltage,
        ROW_NUMBER() OVER (ORDER BY voltage, id) AS rn,
        ROUND(AVG(site_installed_kw) OVER (ORDER BY voltage, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS site_installed_kw_avg_4,
        COUNT(*) OVER (PARTITION BY quality, site_id) AS rows_in_quality_site_id
    FROM combined_details
    WINDOW w_site_tariff_read_at_month_by AS (PARTITION BY site_tariff, DATE_TRUNC('MONTH', read_at) ORDER BY read_at DESC NULLS FIRST, id), w_site_zone AS (PARTITION BY site_zone), w_all_by_read_at AS (ORDER BY read_at, id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM with_trends
    WHERE read_at_seq = 1
      AND quality_site_id_avg_voltage BETWEEN 218.8 AND 231.0
)
-- Final projection
SELECT
    id,
    site_id,
    voltage,
    quality,
    site_tariff,
    min_site_installed_kw_so_far,
    first_known_read_at,
    read_at_seq,
    site_installed_kw_avg_3_prior,
    read_at_quintile,
    read_at_quartile,
    site_tariff_sum_site_installed_kw,
    quality_site_id_avg_voltage,
    rn,
    site_installed_kw_avg_4,
    rows_in_quality_site_id,
    CONCAT_WS(': ', site_zone, site_tariff) AS site_zone_site_tariff_label
FROM first_rows
WHERE site_installed_kw >= 96.9
ORDER BY read_at_quartile, site_tariff_sum_site_installed_kw DESC NULLS FIRST
