WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE (kwh <= 90.6 OR read_at >= TIMESTAMP '2025-02-06 12:00:00')
),
stats AS (
    -- Group-level totals
    SELECT
        quality,
        quality_len,
        site_id,
        COUNT(*) FILTER (WHERE quality NOT IN ('BAD', 'EST')) AS n_quality_not_in_bad_est,
        MAX(quality_len) AS max_quality_len,
        COUNT(*) AS row_count,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE quality = 'BAD') AS n_quality_bad
    FROM prepared_readings
    GROUP BY quality, quality_len, site_id
),
joined_site AS (
    -- Join sites
    SELECT
        s.quality,
        s.quality_len,
        s.site_id,
        s.n_quality_not_in_bad_est,
        s.max_quality_len,
        s.distinct_site_ids,
        s.distinct_ids,
        s.n_quality_bad,
        si.installed_kw AS site_installed_kw,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM stats s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    quality,
    quality_len,
    n_quality_not_in_bad_est,
    max_quality_len,
    distinct_site_ids,
    distinct_ids,
    site_installed_kw,
    site_name,
    site_commissioned_on
FROM joined_site
ORDER BY n_quality_not_in_bad_est, site_name DESC
