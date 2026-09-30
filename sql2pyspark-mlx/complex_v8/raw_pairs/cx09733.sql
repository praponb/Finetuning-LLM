WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality = 'EST'
      AND kwh < 111.9
),
joined_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        si.installed_kw AS site_installed_kw,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM base_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        kwh - LAG(kwh) OVER (PARTITION BY MONTH(site_commissioned_on) ORDER BY voltage DESC, id) AS kwh_change,
        DENSE_RANK() OVER (PARTITION BY MONTH(site_commissioned_on) ORDER BY voltage DESC) AS voltage_dense_rank,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id, YEAR(site_commissioned_on)), 2) AS kwh_vs_site_id_site_commissioned_on_year_avg,
        MIN(site_installed_kw) OVER w_site_id AS site_id_min_site_installed_kw,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id, quality ORDER BY voltage ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_3,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        site_installed_kw - LAG(site_installed_kw) OVER (PARTITION BY site_id, quality ORDER BY voltage ASC, id) AS site_installed_kw_change,
        ROW_NUMBER() OVER w_site_commissioned_on_month_s AS rn
    FROM joined_site
    WINDOW w_site_id AS (PARTITION BY site_id), w_site_commissioned_on_month_s AS (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), site_id ORDER BY site_commissioned_on ASC, id)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        voltage,
        site_installed_kw,
        site_name,
        site_commissioned_on,
        voltage_dense_rank,
        kwh_vs_site_id_site_commissioned_on_year_avg,
        site_id_min_site_installed_kw,
        distinct_quality_in_site_id,
        kwh_share_of_site_id,
        site_installed_kw_change,
        FIRST_VALUE(voltage_dense_rank) IGNORE NULLS OVER (PARTITION BY rn, quality ORDER BY site_installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage_dense_rank,
        ROUND(kwh_change / NULLIF(SUM(kwh_change) OVER (PARTITION BY rn, voltage_dense_rank), 0), 4) AS kwh_change_share_of_rn_voltage_dense_rank,
        FIRST_VALUE(kwh_share_of_site_id) OVER (PARTITION BY rn, quality ORDER BY site_installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh_share_of_site_id,
        SUM(site_installed_kw_change) OVER (PARTITION BY rn, quality ORDER BY site_installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_site_installed_kw_change
    FROM with_windows
)
-- Final projection
SELECT
    id,
    site_installed_kw,
    site_name,
    site_commissioned_on,
    voltage_dense_rank,
    site_id_min_site_installed_kw,
    distinct_quality_in_site_id,
    site_installed_kw_change,
    kwh_change_share_of_rn_voltage_dense_rank,
    first_kwh_share_of_site_id,
    DATE_FORMAT(site_commissioned_on, 'MMM yyyy') AS site_commissioned_on_label
FROM ranked
ORDER BY site_installed_kw_change NULLS LAST, site_commissioned_on DESC
