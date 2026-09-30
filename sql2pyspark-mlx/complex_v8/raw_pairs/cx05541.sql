WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT *
    FROM meter_readings
    WHERE kwh >= 30.2
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.*,
        si.tariff AS site_tariff,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id AND si.installed_kw > 141.6
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        site_tariff,
        site_commissioned_on,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_zone), 0), 4) AS voltage_share_of_site_zone,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_tariff ORDER BY kwh DESC), 3) AS kwh_percent_rank,
        SUM(voltage) OVER w_site_commissioned_on_year_si AS site_commissioned_on_year_site_tariff_sum_voltag,
        ROUND(CUME_DIST() OVER (ORDER BY site_commissioned_on DESC NULLS FIRST, id), 3) AS site_commissioned_on_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_tariff, site_id ORDER BY voltage DESC), 3) AS voltage_percent_rank,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY site_tariff ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        SUM(kwh) OVER w_site_zone AS site_zone_sum_kwh,
        MIN(voltage) OVER w_site_zone AS site_zone_min_voltage,
        ROUND(CUME_DIST() OVER (PARTITION BY site_tariff, DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY site_commissioned_on DESC), 3) AS site_commissioned_on_cume_dist_2,
        ROUND(CUME_DIST() OVER (ORDER BY site_commissioned_on DESC NULLS FIRST), 3) AS site_commissioned_on_cume_dist_3,
        kwh - LAG(kwh) OVER (PARTITION BY site_tariff ORDER BY kwh DESC, id) AS kwh_change,
        ROUND(AVG(voltage) OVER (PARTITION BY site_tariff, site_id ORDER BY voltage DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_3_prior,
        LAST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY site_tariff ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage
    FROM with_parent_site
    WINDOW w_site_commissioned_on_year_si AS (PARTITION BY YEAR(site_commissioned_on), site_tariff), w_site_zone AS (PARTITION BY site_zone)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        INITCAP(TRIM(site_tariff)) AS site_tariff_clean,
        ROUND(site_zone_sum_kwh / NULLIF(last_known_voltage, 0), 3) AS site_zone_sum_kwh_per_last_known_voltage
    FROM sequenced
    WHERE site_tariff IS NOT NULL
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(site_commissioned_on) AS site_commissioned_on_year,
        quality,
        SUM(CASE WHEN site_tariff NOT IN ('TOU', 'FLAT') THEN 1 ELSE 0 END) AS n_site_tariff_not_in_tou_flat,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        ROUND(SUM(site_zone_min_voltage) / NULLIF(SUM(site_commissioned_on_cume_dist_3), 0), 3) AS site_zone_min_voltage_to_site_commissioned_on_cu,
        SUM(voltage_percent_rank) AS total_voltage_percent_rank,
        MAX(site_commissioned_on_cume_dist_2) AS max_site_commissioned_on_cume_dist_2
    FROM tagged
    GROUP BY YEAR(site_commissioned_on), quality
)
-- Final projection
SELECT
    site_commissioned_on_year,
    n_site_tariff_not_in_tou_flat,
    distinct_qualitys,
    site_zone_min_voltage_to_site_commissioned_on_cu,
    max_site_commissioned_on_cume_dist_2,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
FROM totals
