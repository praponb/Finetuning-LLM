WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        installed_kw,
        commissioned_on,
        LOWER(REGEXP_REPLACE(TRIM(site_name), ' ', '_')) AS site_name_slug
    FROM sites
    WHERE zone IN ('Z3', 'Z4')
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(commissioned_on, 'yyyy-MM') AS commissioned_on_ym,
        ROUND(100 * COUNT(CASE WHEN LENGTH(TRIM(site_name)) >= 6 THEN 1 END) / COUNT(*), 1) AS pct_length_trim_site_name_6,
        ROUND(100 * COUNT(CASE WHEN LENGTH(TRIM(site_name)) >= 4 THEN 1 END) / COUNT(*), 1) AS pct_length_trim_site_name_4,
        SUM(CASE WHEN site_name IS NOT NULL THEN 1 ELSE 0 END) AS n_site_name_is_not_null,
        PERCENTILE_APPROX(installed_kw, 0.5) AS median_installed_kw,
        MAX(installed_kw) AS max_installed_kw
    FROM base
    GROUP BY DATE_FORMAT(commissioned_on, 'yyyy-MM')
),
enriched AS (
    -- Label and score rows
    SELECT
        commissioned_on_ym,
        pct_length_trim_site_name_6,
        pct_length_trim_site_name_4,
        n_site_name_is_not_null,
        median_installed_kw,
        max_installed_kw,
        LENGTH(TRIM(commissioned_on_ym)) AS commissioned_on_ym_len,
        LENGTH(TRIM(commissioned_on_ym)) AS commissioned_on_ym_len_2,
        ABS(max_installed_kw - 203.0) AS max_installed_kw_distance
    FROM summary
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(max_installed_kw, median_installed_kw) AS least_max_installed_kw_median_installed_kw,
        LOWER(REGEXP_REPLACE(TRIM(commissioned_on_ym), ' ', '_')) AS commissioned_on_ym_slug,
        SUBSTRING(TRIM(commissioned_on_ym), 1, 2) AS commissioned_on_ym_prefix
    FROM enriched
    WHERE commissioned_on_ym IS NOT NULL
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        median_installed_kw - max_installed_kw AS median_installed_kw_minus_max_installed_kw
    FROM enriched_2
    WHERE commissioned_on_ym_slug IS NOT NULL
)
SELECT
    commissioned_on_ym,
    pct_length_trim_site_name_6,
    pct_length_trim_site_name_4,
    n_site_name_is_not_null,
    median_installed_kw,
    max_installed_kw,
    commissioned_on_ym_len_2,
    commissioned_on_ym_slug,
    commissioned_on_ym_prefix,
    median_installed_kw_minus_max_installed_kw,
    ABS(median_installed_kw - 351.5) AS median_installed_kw_distance,
    SUBSTRING(TRIM(commissioned_on_ym_slug), 1, 2) AS commissioned_on_ym_slug_prefix
FROM tagged
ORDER BY commissioned_on_ym_slug_prefix DESC
