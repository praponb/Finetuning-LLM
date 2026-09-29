WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality_slug,
        si.installed_kw AS site_installed_kw
    FROM scoped s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY site_id ORDER BY kwh NULLS LAST, id) AS kwh_dense_rank,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        DENSE_RANK() OVER w_site_id_by_voltage_ties AS voltage_dense_rank,
        MAX(kwh) OVER (PARTITION BY site_id) AS site_id_max_kwh,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY site_installed_kw, id), 3) AS site_installed_kw_percent_rank,
        COUNT(*) OVER (PARTITION BY site_id ORDER BY kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY kwh DESC), 3) AS kwh_percent_rank,
        DENSE_RANK() OVER (PARTITION BY site_id ORDER BY site_installed_kw) AS site_installed_kw_dense_rank,
        SUM(site_installed_kw) OVER (PARTITION BY site_id) AS site_id_sum_site_installed_kw,
        NTILE(10) OVER (PARTITION BY site_id ORDER BY site_installed_kw, id) AS site_installed_kw_decile,
        ROUND(site_installed_kw - AVG(site_installed_kw) OVER (PARTITION BY site_id), 2) AS site_installed_kw_vs_site_id_avg,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER (PARTITION BY site_id), 0), 4) AS site_installed_kw_share_of_site_id
    FROM with_details_site
    WINDOW w_site_id AS (PARTITION BY site_id), w_site_id_by_voltage_ties AS (PARTITION BY site_id ORDER BY voltage)
),
per_group AS (
    -- Group-level totals
    SELECT
        site_id,
        site_installed_kw_dense_rank,
        COUNT(*) AS row_count,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        PERCENTILE_APPROX(site_installed_kw_vs_site_id_avg, 0.5) AS median_site_installed_kw_vs_site_id_avg
    FROM with_trends
    WHERE kwh_dense_rank <= 6
    GROUP BY 1, 2
    HAVING MAX(running_count) > 9
)
-- Final projection
SELECT
    site_id,
    row_count,
    distinct_site_ids,
    median_site_installed_kw_vs_site_id_avg
FROM per_group
ORDER BY row_count DESC
