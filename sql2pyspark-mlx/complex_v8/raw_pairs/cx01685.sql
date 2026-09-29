WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        ROUND(kwh * 0.9, 2) AS kwh_adj
    FROM meter_readings
    WHERE voltage BETWEEN 218.2 AND 233.6
      AND MONTH(read_at) IN (1, 2)
),
combined_details_site AS (
    -- Join sites
    SELECT s.*, si.site_name
    FROM recent s
    JOIN sites si ON si.id = s.site_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(quality_ref, '#', CAST(site_id AS STRING)) AS quality_ref_ref,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
    FROM combined_details_site
    WHERE kwh_adj BETWEEN 45.0 AND 144.0
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.quality_ref,
        s.kwh_adj,
        s.site_name,
        s.quality_ref_ref,
        s.days_since_read_at,
        si.installed_kw AS site_installed_kw
    FROM tagged s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw < 284.0
),
quality_stats AS (
    -- Avg site_installed_kw per quality
    SELECT quality, AVG(site_installed_kw) AS avg_site_installed_kw
    FROM enriched_site
    GROUP BY quality
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_site_installed_kw,
        ROUND(s.site_installed_kw - g.avg_site_installed_kw, 2) AS site_installed_kw_minus_avg
    FROM enriched_site s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.site_installed_kw > g.avg_site_installed_kw
),
grouped AS (
    -- Group-level totals
    SELECT
        quality,
        COUNT(*) AS n_rows,
        SUM(CASE WHEN LENGTH(TRIM(site_name)) >= 7 THEN 1 ELSE 0 END) AS n_length_trim_site_name_7,
        ROUND(100 * COUNT(CASE WHEN quality IN ('BAD', 'OK') THEN 1 END) / COUNT(*), 1) AS pct_quality_in_bad_ok
    FROM outliers
    GROUP BY quality
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT n_rows, quality, pct_quality_in_bad_ok
    FROM grouped
) src
PIVOT (
    AVG(pct_quality_in_bad_ok)
    FOR quality IN ('OK', 'BAD')
)
ORDER BY n_rows
