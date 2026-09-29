WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        zone,
        tariff,
        commissioned_on,
        CASE WHEN installed_kw >= 284.0 THEN 'HIGH' WHEN installed_kw >= 141.6 THEN 'MEDIUM' ELSE 'LOW' END AS installed_kw_tier,
        ROUND(installed_kw * 1.07, 2) AS installed_kw_adj,
        DATE_TRUNC('WEEK', commissioned_on) AS commissioned_on_week
    FROM sites
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(commissioned_on_week) AS commissioned_on_week_year,
        COUNT(*) AS cnt,
        COUNT(DISTINCT installed_kw_tier) AS distinct_installed_kw_tiers,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(installed_kw_adj), 2) AS avg_installed_kw_adj
    FROM prepared
    GROUP BY YEAR(commissioned_on_week)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE avg_installed_kw_adj > (SELECT AVG(avg_installed_kw_adj) FROM per_group)
)
-- Final projection
SELECT
    commissioned_on_week_year,
    cnt,
    distinct_installed_kw_tiers,
    distinct_ids,
    avg_installed_kw_adj,
    FLOOR(cnt / 2) * 2 AS cnt_bucket,
    ROUND(avg_installed_kw_adj / NULLIF(cnt, 0), 3) AS avg_installed_kw_adj_per_cnt
FROM screened
ORDER BY cnt_bucket DESC, distinct_ids DESC NULLS FIRST
