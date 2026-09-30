WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label
    FROM meter_readings
),
joined_site AS (
    -- Join sites
    SELECT s.*, si.tariff AS site_tariff, si.installed_kw AS site_installed_kw
    FROM scoped s
    JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_tariff), 0), 4) AS voltage_share_of_site_tariff,
        FIRST_VALUE(kwh) OVER (PARTITION BY quality ORDER BY site_installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        SUM(site_installed_kw) OVER (PARTITION BY quality ORDER BY site_installed_kw ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS site_installed_kw_sum_7,
        ROUND(site_installed_kw - AVG(site_installed_kw) OVER w_read_at_month, 2) AS site_installed_kw_vs_read_at_month_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality ORDER BY site_installed_kw ASC), 3) AS site_installed_kw_percent_rank,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY MONTH(read_at)), 0), 4) AS kwh_share_of_read_at_month,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY TO_DATE(read_at) ORDER BY site_installed_kw, id))) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(AVG(voltage) OVER (PARTITION BY TO_DATE(read_at) ORDER BY site_installed_kw, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_3_prior,
        MAX(kwh) OVER (PARTITION BY quality, site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far,
        SIZE(COLLECT_SET(quality) OVER w_read_at_month) AS distinct_quality_in_read_at_month,
        ROUND(site_installed_kw - AVG(site_installed_kw) OVER (PARTITION BY site_tariff), 2) AS site_installed_kw_vs_site_tariff_avg,
        ROUND(AVG(site_installed_kw) OVER w_site_tariff, 2) AS site_tariff_avg_site_installed_kw,
        NTILE(4) OVER (ORDER BY voltage DESC NULLS FIRST, id) AS voltage_quartile,
        LEAD(voltage, 2) OVER (PARTITION BY quality ORDER BY site_installed_kw ASC, id) AS next2_voltage,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY quality ORDER BY site_installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh
    FROM joined_site
    WINDOW w_read_at_month AS (PARTITION BY MONTH(read_at)), w_site_tariff AS (PARTITION BY site_tariff)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
    FROM with_windows
    WHERE site_id <= 4
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id, quality), 2) AS kwh_vs_site_id_quality_avg,
        ROUND(AVG(next2_voltage) OVER (PARTITION BY site_id, quality), 2) AS site_id_quality_avg_next2_voltage,
        COUNT(*) OVER w_site_id_quality AS rows_in_site_id_quality,
        FIRST_VALUE(read_at) OVER (PARTITION BY distinct_quality_in_read_at_month, quality ORDER BY voltage_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        ROUND(CUME_DIST() OVER (ORDER BY site_installed_kw_sum_7, id), 3) AS site_installed_kw_sum_7_cume_dist
    FROM enriched
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality)
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    site_tariff,
    site_installed_kw,
    first_kwh,
    site_installed_kw_vs_read_at_month_avg,
    minutes_since_prev_read_at,
    voltage_avg_3_prior,
    max_kwh_so_far,
    distinct_quality_in_read_at_month,
    site_installed_kw_vs_site_tariff_avg,
    site_tariff_avg_site_installed_kw,
    voltage_quartile,
    next2_voltage,
    first_known_kwh,
    days_since_read_at,
    voltage_per_kwh,
    site_id_quality_avg_next2_voltage,
    first_read_at,
    site_installed_kw_sum_7_cume_dist,
    CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
    ROUND(rows_in_site_id_quality / NULLIF(first_known_kwh, 0), 3) AS rows_in_site_id_quality_per_first_known_kwh
FROM ranked
ORDER BY quality_group DESC NULLS FIRST
