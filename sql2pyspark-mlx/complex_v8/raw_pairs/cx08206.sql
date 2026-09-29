WITH scoped_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        ADD_MONTHS(commissioned_on, 6) AS commissioned_on_plus_6m
    FROM sites
    WHERE (site_name IS NOT NULL OR installed_kw > 93.3)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        RANK() OVER (PARTITION BY zone, YEAR(commissioned_on_plus_6m) ORDER BY installed_kw ASC) AS installed_kw_rank,
        ROUND(PERCENT_RANK() OVER w_zone_commissioned_on_plus_6m, 3) AS installed_kw_percent_rank,
        ROUND(installed_kw - AVG(installed_kw) OVER (PARTITION BY zone, tariff), 2) AS installed_kw_vs_zone_tariff_avg
    FROM scoped_sites
    WINDOW w_zone_commissioned_on_plus_6m AS (PARTITION BY zone, YEAR(commissioned_on_plus_6m) ORDER BY installed_kw ASC)
),
tariff_stats AS (
    -- Median installed_kw_vs_zone_tariff_avg per tariff
    SELECT tariff, PERCENTILE_APPROX(installed_kw_vs_zone_tariff_avg, 0.5) AS median_installed_kw_vs_zone_tariff_avg
    FROM with_trends
    GROUP BY tariff
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_installed_kw_vs_zone_tariff_avg,
        ROUND(s.installed_kw_vs_zone_tariff_avg - g.median_installed_kw_vs_zone_tariff_avg, 2) AS installed_kw_vs_zone_tariff_avg_minus_median
    FROM with_trends s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.installed_kw_vs_zone_tariff_avg >= g.median_installed_kw_vs_zone_tariff_avg
)
SELECT
    id,
    zone,
    tariff,
    installed_kw,
    installed_kw_rank,
    installed_kw_percent_rank,
    installed_kw_vs_zone_tariff_avg,
    median_installed_kw_vs_zone_tariff_avg,
    installed_kw_vs_zone_tariff_avg_minus_median,
    DATE_FORMAT(commissioned_on, 'MMM yyyy') AS commissioned_on_label
FROM compared
ORDER BY tariff DESC
