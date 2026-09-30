WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        voltage + kwh AS voltage_plus_kwh,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE voltage >= 223.0
      AND kwh < 90.6
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality_or_unknown,
        s.voltage_plus_kwh,
        s.quality_slug,
        si.installed_kw AS site_installed_kw,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM scoped_readings s
    JOIN sites si ON si.id = s.site_id
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        kwh,
        quality_or_unknown,
        quality_slug,
        site_installed_kw,
        site_commissioned_on,
        DATEDIFF(DATE '2025-06-30', site_commissioned_on) AS days_since_site_commissioned_on,
        DATEDIFF(DATE '2026-01-01', site_commissioned_on) AS days_since_site_commissioned_on_2
    FROM with_parent
),
summary AS (
    -- Group-level totals
    SELECT
        YEAR(site_commissioned_on) AS site_commissioned_on_year,
        quality_or_unknown,
        ROUND(SUM(kwh) / NULLIF(SUM(site_installed_kw), 0), 3) AS kwh_to_site_installed_kw_ratio,
        MAX(site_commissioned_on) AS last_site_commissioned_on,
        SUM(days_since_site_commissioned_on_2) AS total_days_since_site_commissioned_on_2,
        ROUND(SUM(site_installed_kw), 2) AS total_site_installed_kw,
        DATEDIFF(MAX(site_commissioned_on), MIN(site_commissioned_on)) AS site_commissioned_on_span_days,
        ROUND(AVG(days_since_site_commissioned_on), 2) AS avg_days_since_site_commissioned_on
    FROM derived
    WHERE quality_slug IS NOT NULL
    GROUP BY YEAR(site_commissioned_on), quality_or_unknown
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(total_site_installed_kw * 1.07, 2) AS total_site_installed_kw_adj
    FROM summary
    WHERE site_commissioned_on_span_days <= 560
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(last_site_commissioned_on), site_commissioned_on_year ORDER BY site_commissioned_on_span_days), 3) AS site_commissioned_on_span_days_cume_dist,
        RANK() OVER w_last_site_commissioned_on_mo_2 AS site_commissioned_on_span_days_rank,
        DENSE_RANK() OVER (PARTITION BY MONTH(last_site_commissioned_on), site_commissioned_on_year ORDER BY site_commissioned_on_span_days, site_commissioned_on_year, quality_or_unknown) AS site_commissioned_on_span_days_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(last_site_commissioned_on), site_commissioned_on_year ORDER BY site_commissioned_on_span_days), 3) AS site_commissioned_on_span_days_percent_rank
    FROM enriched
    WINDOW w_last_site_commissioned_on_mo_2 AS (PARTITION BY MONTH(last_site_commissioned_on), site_commissioned_on_year ORDER BY site_commissioned_on_span_days, site_commissioned_on_year, quality_or_unknown), w_last_site_commissioned_on_mo AS (PARTITION BY MONTH(last_site_commissioned_on), site_commissioned_on_year ORDER BY site_commissioned_on_span_days)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_commissioned_on_year, site_commissioned_on_span_days_dense_rank, quality_or_unknown, total_site_installed_kw_adj
    FROM sequenced
) src
PIVOT (
    MIN(total_site_installed_kw_adj)
    FOR quality_or_unknown IN ('UNKNOWN', 'EST', 'OK')
)
ORDER BY site_commissioned_on_year, site_commissioned_on_span_days_dense_rank
