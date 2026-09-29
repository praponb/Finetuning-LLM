WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group_2
    FROM meter_readings
    WHERE voltage >= 222.3
      AND read_at >= TIMESTAMP '2025-02-06 11:00:00'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        voltage,
        quality,
        RANK() OVER (PARTITION BY site_id, quality_group_2 ORDER BY voltage ASC) AS voltage_rank,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality_group_2)) AS distinct_site_id_in_quality_group_2,
        MAX(kwh) OVER (PARTITION BY quality_group_2) AS quality_group_2_max_kwh,
        ROUND(voltage - AVG(voltage) OVER w_quality_group_2, 2) AS voltage_vs_quality_group_2_avg
    FROM base
    WINDOW w_quality_group_2 AS (PARTITION BY quality_group_2)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN quality_group_2_max_kwh >= 780.0 THEN 'A' WHEN quality_group_2_max_kwh >= 540.0 THEN 'B' ELSE 'C' END AS quality_group_2_max_kwh_tier,
        distinct_site_id_in_quality_group_2,
        site_id,
        SUM(CASE WHEN quality = 'OK' THEN voltage_vs_quality_group_2_avg ELSE 0 END) AS voltage_vs_quality_group_2_avg_quality_ok,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        COUNT(*) FILTER (WHERE quality IN ('BAD', 'OK')) AS n_quality_in_bad_ok
    FROM with_trends
    GROUP BY 1, 2, 3
    HAVING SUM(voltage_vs_quality_group_2_avg) > -98.0
),
with_details_site AS (
    -- Join sites
    SELECT
        s.quality_group_2_max_kwh_tier,
        s.distinct_site_id_in_quality_group_2,
        s.site_id,
        s.voltage_vs_quality_group_2_avg_quality_ok,
        s.quality_list,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff
    FROM summary s
    JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    quality_group_2_max_kwh_tier,
    distinct_site_id_in_quality_group_2,
    site_id,
    quality_list,
    site_tariff,
    DATEDIFF(DATE '2025-06-30', site_commissioned_on) AS days_since_site_commissioned_on,
    COALESCE(site_installed_kw, 0.0) AS site_installed_kw_filled
FROM with_details_site
WHERE site_id <= 6
