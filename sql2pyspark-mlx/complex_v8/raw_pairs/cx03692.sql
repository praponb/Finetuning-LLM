WITH filtered_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        DATE_TRUNC('WEEK', commissioned_on) AS commissioned_on_week,
        MONTH(commissioned_on) AS commissioned_on_month
    FROM sites
    WHERE (installed_kw < 284.0 OR zone <> 'Z2')
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        site_name,
        tariff,
        installed_kw,
        commissioned_on_week,
        commissioned_on_month,
        ADD_MONTHS(commissioned_on, 6) AS commissioned_on_plus_6m,
        LOWER(REGEXP_REPLACE(TRIM(site_name), ' ', '_')) AS site_name_slug
    FROM filtered_sites
),
scored AS (
    -- Label and score rows
    SELECT *, COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM enriched
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE m.quality IS NOT NULL) FROM meter_readings m WHERE m.site_id = s.id) AS reading_quality_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.id) AS reading_count
    FROM scored s
)
SELECT
    reading_count,
    reading_quality_count,
    COALESCE(tariff, 'ALL') AS tariff,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(reading_quality_count) AS max_reading_quality_count
FROM profiled
GROUP BY GROUPING SETS ((reading_count, reading_quality_count, tariff), (reading_count), (reading_quality_count), ())
ORDER BY reading_count NULLS LAST, reading_quality_count NULLS LAST, tariff NULLS LAST
