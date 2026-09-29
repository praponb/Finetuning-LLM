WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE (voltage > 222.6 OR kwh >= 48.2)
),
enriched_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM scoped s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        quality,
        site_commissioned_on,
        MAX(voltage) OVER w_quality_site_id AS quality_site_id_max_voltage,
        MAX(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), site_id) AS site_commissioned_on_month_site_id_max_voltage,
        SUM(voltage) OVER (PARTITION BY quality, site_id ORDER BY site_commissioned_on DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS voltage_sum_6_prior,
        LAST_VALUE(site_commissioned_on) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_site_commissioned_on,
        MIN(voltage) OVER w_site_zone AS site_zone_min_voltage,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality, site_id), 0), 4) AS voltage_share_of_quality_site_id,
        SUM(voltage) OVER (PARTITION BY quality, site_id ORDER BY site_commissioned_on DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS voltage_sum_3_prior,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_site_commissioned_on_month_s, 0), 4) AS voltage_share_of_site_commissioned_on_month_site,
        NTILE(4) OVER (ORDER BY site_commissioned_on DESC, id) AS site_commissioned_on_quartile,
        ROUND(voltage - AVG(voltage) OVER w_quality_site_id, 2) AS voltage_vs_quality_site_id_avg,
        LAST_VALUE(site_commissioned_on) OVER (ORDER BY site_commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_site_commissioned_on,
        LAST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY site_zone ORDER BY site_commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        ROUND(AVG(voltage) OVER w_site_zone, 2) AS site_zone_avg_voltage,
        ROUND(AVG(voltage) OVER (PARTITION BY quality, site_id ORDER BY site_commissioned_on DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_2_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY quality, site_id ORDER BY site_commissioned_on DESC NULLS FIRST), 3) AS site_commissioned_on_cume_dist
    FROM enriched_site
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id), w_site_zone AS (PARTITION BY site_zone), w_site_commissioned_on_month_s AS (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), site_id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(site_zone_min_voltage / 500) * 500 AS site_zone_min_voltage_bucket,
        CASE WHEN site_zone_avg_voltage >= 234.5 THEN 'A' WHEN site_zone_avg_voltage >= 222.2 THEN 'B' ELSE 'C' END AS site_zone_avg_voltage_tier,
        TRUNC(site_commissioned_on, 'MM') AS site_commissioned_on_month_start
    FROM with_windows
    WHERE voltage_sum_6_prior > 420.0
)
SELECT
    id,
    quality,
    site_commissioned_on,
    quality_site_id_max_voltage,
    voltage_sum_6_prior,
    last_known_site_commissioned_on,
    voltage_sum_3_prior,
    voltage_share_of_site_commissioned_on_month_site,
    site_commissioned_on_quartile,
    last_site_commissioned_on,
    last_known_voltage,
    site_zone_avg_voltage,
    site_commissioned_on_cume_dist,
    site_zone_min_voltage_bucket,
    site_zone_avg_voltage_tier,
    site_commissioned_on_month_start,
    COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
FROM labeled
ORDER BY last_site_commissioned_on
