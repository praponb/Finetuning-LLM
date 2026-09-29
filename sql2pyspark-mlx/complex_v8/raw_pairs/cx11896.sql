WITH scoped AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        tariff,
        installed_kw,
        commissioned_on,
        SUBSTRING(TRIM(zone), 1, 3) AS zone_prefix,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket
    FROM sites
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        site_name,
        tariff,
        installed_kw,
        commissioned_on,
        zone_prefix,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', commissioned_on), tariff) AS rows_in_commissioned_on_month_tariff,
        ROUND(PERCENT_RANK() OVER (ORDER BY commissioned_on), 3) AS commissioned_on_percent_rank,
        MIN(installed_kw_bucket) OVER (PARTITION BY DATE_TRUNC('MONTH', commissioned_on), tariff) AS commissioned_on_month_tariff_min_installed_kw_bu,
        ROUND(CUME_DIST() OVER (ORDER BY commissioned_on), 3) AS commissioned_on_cume_dist,
        FIRST_VALUE(commissioned_on) OVER (PARTITION BY tariff ORDER BY commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_commissioned_on,
        MAX(installed_kw_bucket) OVER (PARTITION BY tariff) AS tariff_max_installed_kw_bucket,
        LEAD(installed_kw) OVER w_all_by_commissioned_on AS next_installed_kw,
        ROUND(installed_kw - AVG(installed_kw) OVER (PARTITION BY tariff), 2) AS installed_kw_vs_tariff_avg,
        LEAD(installed_kw_bucket, 1, 0) OVER (ORDER BY commissioned_on, id) AS next_installed_kw_bucket,
        ROUND(installed_kw_bucket / NULLIF(SUM(installed_kw_bucket) OVER (PARTITION BY DATE_TRUNC('MONTH', commissioned_on), tariff), 0), 4) AS installed_kw_bucket_share_of_commissioned_on_mon,
        NTH_VALUE(commissioned_on, 2) OVER (PARTITION BY tariff ORDER BY installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_commissioned_on
    FROM scoped
    WINDOW w_all_by_commissioned_on AS (ORDER BY commissioned_on, id)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        tariff,
        commissioned_on,
        zone_prefix,
        rows_in_commissioned_on_month_tariff,
        commissioned_on_month_tariff_min_installed_kw_bu,
        commissioned_on_cume_dist,
        first_commissioned_on,
        tariff_max_installed_kw_bucket,
        next_installed_kw,
        installed_kw_vs_tariff_avg,
        installed_kw_bucket_share_of_commissioned_on_mon,
        second_commissioned_on,
        CASE WHEN tariff IN ('EV', 'TOU') THEN 'group_a' WHEN tariff = 'FLAT' THEN 'group_b' ELSE 'other' END AS tariff_group,
        LOWER(REGEXP_REPLACE(TRIM(zone_prefix), ' ', '_')) AS zone_prefix_slug,
        DATE_TRUNC('QUARTER', commissioned_on) AS commissioned_on_quarter
    FROM ranked
    WHERE second_commissioned_on >= DATE '2024-06-27'
)
-- Final projection
SELECT
    id,
    commissioned_on,
    zone_prefix,
    rows_in_commissioned_on_month_tariff,
    commissioned_on_month_tariff_min_installed_kw_bu,
    first_commissioned_on,
    next_installed_kw,
    installed_kw_vs_tariff_avg,
    second_commissioned_on,
    tariff_group,
    zone_prefix_slug,
    commissioned_on_quarter,
    FLOOR(next_installed_kw / 100) * 100 AS next_installed_kw_bucket,
    FLOOR(next_installed_kw / 100) * 100 AS next_installed_kw_bucket_2
FROM enriched
ORDER BY id DESC, tariff_group DESC
LIMIT 10
