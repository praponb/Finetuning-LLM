WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality <> 'OK'
      AND kwh BETWEEN 24.5 AND 110.5
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on,
        si.site_name
    FROM filtered_readings s
    JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_site_zone_by_voltage, 3) AS voltage_cume_dist,
        ROUND(AVG(kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), quality ORDER BY voltage ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_3_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_zone ORDER BY voltage), 3) AS voltage_percent_rank,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality), 2) AS kwh_vs_quality_avg,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), quality ORDER BY voltage ASC), 3) AS voltage_percent_rank_2,
        MIN(voltage) OVER (PARTITION BY site_zone ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_so_far,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY quality), 2) AS voltage_vs_quality_avg
    FROM with_details_site
    WINDOW w_site_zone_by_voltage AS (PARTITION BY site_zone ORDER BY voltage, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(TRIM(quality)) AS quality_clean,
        CASE WHEN site_zone IN ('Z2', 'Z4') THEN 'group_a' WHEN site_zone = 'Z3' THEN 'group_b' ELSE 'other' END AS site_zone_group,
        COALESCE(voltage_percent_rank_2, 0.0) AS voltage_percent_rank_2_filled
    FROM windowed
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        kwh,
        site_zone,
        site_commissioned_on,
        site_name,
        voltage_cume_dist,
        kwh_avg_3_prior,
        voltage_percent_rank,
        kwh_vs_quality_avg,
        rows_in_quality,
        voltage_vs_quality_avg,
        quality_clean,
        voltage_percent_rank_2_filled,
        COUNT(*) OVER (PARTITION BY quality, site_zone) AS rows_in_quality_site_zone,
        MAX(voltage) OVER (PARTITION BY quality, site_zone) AS quality_site_zone_max_voltage,
        MAX(rows_in_quality) OVER (PARTITION BY quality ORDER BY min_voltage_so_far DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rows_in_quality_max_2_prior,
        RANK() OVER (ORDER BY min_voltage_so_far) AS min_voltage_so_far_rank,
        LEAD(site_commissioned_on) OVER (ORDER BY min_voltage_so_far, id) AS next_site_commissioned_on,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality, site_zone)) AS distinct_site_id_in_quality_site_zone
    FROM annotated
)
-- Final projection
SELECT
    id,
    kwh,
    site_zone,
    site_commissioned_on,
    site_name,
    voltage_cume_dist,
    voltage_percent_rank,
    kwh_vs_quality_avg,
    rows_in_quality,
    voltage_vs_quality_avg,
    quality_clean,
    voltage_percent_rank_2_filled,
    quality_site_zone_max_voltage,
    next_site_commissioned_on,
    distinct_site_id_in_quality_site_zone,
    DATE_TRUNC('WEEK', site_commissioned_on) AS site_commissioned_on_week,
    DATEDIFF(DATE '2026-01-01', site_commissioned_on) AS days_since_site_commissioned_on
FROM ranked
ORDER BY id DESC, kwh_vs_quality_avg
LIMIT 20
