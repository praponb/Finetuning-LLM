WITH valid AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug
    FROM sites
    WHERE (LOWER(site_name) LIKE '%ro%' OR installed_kw >= 283.9)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        zone_slug,
        DATEDIFF(commissioned_on, LAG(commissioned_on) OVER (PARTITION BY tariff ORDER BY installed_kw DESC, id)) AS days_since_prev_commissioned_on,
        MAX(installed_kw) OVER (PARTITION BY tariff ORDER BY installed_kw DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS installed_kw_max_3_prior,
        ROUND(installed_kw - AVG(installed_kw) OVER (PARTITION BY zone), 2) AS installed_kw_vs_zone_avg,
        ROUND(CUME_DIST() OVER w_tariff_by_installed_kw, 3) AS installed_kw_cume_dist,
        SUM(installed_kw) OVER w_zone AS zone_sum_installed_kw
    FROM valid
    WINDOW w_tariff_by_installed_kw AS (PARTITION BY tariff ORDER BY installed_kw DESC, id), w_zone AS (PARTITION BY zone)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        installed_kw_cume_dist - installed_kw_max_3_prior AS installed_kw_cume_dist_minus_installed_kw_max_3_,
        CASE WHEN zone IN ('Z4', 'Z2') THEN 'group_a' WHEN zone = 'Z3' THEN 'group_b' ELSE 'other' END AS zone_group
    FROM ranked
),
grouped AS (
    -- Group-level totals
    SELECT
        tariff,
        zone,
        zone_group,
        MIN(days_since_prev_commissioned_on) AS min_days_since_prev_commissioned_on,
        COUNT(*) AS n_rows,
        PERCENTILE_APPROX(installed_kw_cume_dist_minus_installed_kw_max_3_, 0.5) AS median_installed_kw_cume_dist_minus_installed_kw
    FROM scored
    WHERE zone NOT IN ('Z4', 'Z3')
    GROUP BY tariff, zone, zone_group
    HAVING MAX(installed_kw_cume_dist) > 0.1
)
-- Final projection
SELECT
    tariff,
    zone,
    zone_group,
    min_days_since_prev_commissioned_on,
    n_rows,
    median_installed_kw_cume_dist_minus_installed_kw
FROM grouped
WHERE n_rows >= 4
