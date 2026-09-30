WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE quality = 'BAD'
      AND kwh > 20.4
),
combined_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.quality_or_unknown,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM recent_readings s
    JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        voltage,
        quality,
        quality_or_unknown,
        site_installed_kw,
        site_name,
        MAX(site_installed_kw) OVER (PARTITION BY quality, site_id) AS quality_site_id_max_site_installed_kw,
        LAG(voltage, 1, 0) OVER (ORDER BY site_installed_kw DESC, id) AS prev_voltage,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_quality_site_id, 0), 4) AS voltage_share_of_quality_site_id,
        RANK() OVER (PARTITION BY site_id ORDER BY voltage DESC, id) AS voltage_rank,
        LAG(site_installed_kw) OVER w_quality_site_id_by_site_inst AS prev_site_installed_kw,
        MAX(site_installed_kw) OVER (PARTITION BY site_id ORDER BY voltage DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS site_installed_kw_max_4,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        MIN(voltage) OVER (PARTITION BY quality, site_id) AS quality_site_id_min_voltage,
        MAX(voltage) OVER (PARTITION BY quality) AS quality_max_voltage,
        ROUND(site_installed_kw - AVG(site_installed_kw) OVER (PARTITION BY quality, site_id), 2) AS site_installed_kw_vs_quality_site_id_avg,
        COUNT(*) OVER (PARTITION BY quality, site_id) AS rows_in_quality_site_id,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id ORDER BY site_installed_kw DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_6_prior,
        LEAD(site_installed_kw) OVER (PARTITION BY site_id ORDER BY voltage DESC, id) AS next_site_installed_kw
    FROM combined_details_site
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id), w_quality_site_id_by_site_inst AS (PARTITION BY quality, site_id ORDER BY site_installed_kw, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM with_windows
    WHERE voltage_rank <= 5
      AND site_installed_kw >= 93.3
),
summary AS (
    -- Group-level totals
    SELECT
        site_id,
        quality,
        quality_or_unknown,
        MAX(rows_in_quality_site_id) AS max_rows_in_quality_site_id,
        COUNT(DISTINCT quality_or_unknown) AS distinct_quality_or_unknowns,
        MIN(voltage_share_of_quality_site_id) AS min_voltage_share_of_quality_site_id
    FROM top_per_group
    WHERE prev_site_installed_kw > 141.6
    GROUP BY site_id, quality, quality_or_unknown
    HAVING AVG(voltage_avg_6_prior) > 215.2
)
-- Final projection
SELECT
    site_id,
    quality,
    max_rows_in_quality_site_id,
    distinct_quality_or_unknowns,
    min_voltage_share_of_quality_site_id,
    CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
    INITCAP(TRIM(quality)) AS quality_clean
FROM summary
WHERE max_rows_in_quality_site_id <= 17
ORDER BY min_voltage_share_of_quality_site_id
