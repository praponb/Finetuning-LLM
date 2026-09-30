WITH filtered_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        UPPER(TRIM(site_name)) AS site_name_clean,
        ABS(installed_kw - 96.9) AS installed_kw_distance
    FROM sites
    WHERE tariff IN ('TOU', 'EV')
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(installed_kw_distance / NULLIF(installed_kw, 0), 3) AS installed_kw_distance_per_installed_kw,
        DATEDIFF(DATE '2025-12-31', commissioned_on) AS days_since_commissioned_on
    FROM filtered_sites
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(commissioned_on, 'yyyy-MM') AS commissioned_on_ym,
        zone,
        tariff,
        COUNT(*) AS row_count,
        MAX(commissioned_on) AS last_commissioned_on,
        ARRAY_SORT(COLLECT_SET(zone)) AS zone_list
    FROM annotated
    GROUP BY DATE_FORMAT(commissioned_on, 'yyyy-MM'), zone, tariff
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE row_count > (SELECT AVG(row_count) FROM totals)
)
SELECT
    commissioned_on_ym,
    zone,
    tariff,
    row_count,
    last_commissioned_on,
    zone_list,
    ARRAY_JOIN(ARRAY_SORT(zone_list), ',') AS zone_list_text
FROM without_matches
