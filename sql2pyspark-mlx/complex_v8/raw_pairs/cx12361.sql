WITH valid AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        tariff,
        commissioned_on,
        CASE WHEN installed_kw >= 284.0 THEN 'HIGH' WHEN installed_kw >= 141.6 THEN 'MEDIUM' ELSE 'LOW' END AS installed_kw_tier,
        LENGTH(TRIM(site_name)) AS site_name_len,
        DATEDIFF(DATE '2025-12-31', commissioned_on) AS days_since_commissioned_on
    FROM valid
)
SELECT
    COALESCE(installed_kw_tier, 'ALL') AS installed_kw_tier,
    COALESCE(tariff, 'ALL') AS tariff,
    GROUPING_ID() AS grouping_level,
    COUNT(*) FILTER (WHERE tariff NOT IN ('EV', 'TOU')) AS n_tariff_not_in_ev_tou,
    ARRAY_SORT(COLLECT_SET(tariff)) AS tariff_list,
    MAX(site_name_len) AS max_site_name_len
FROM labeled
GROUP BY GROUPING SETS ((installed_kw_tier, tariff), (installed_kw_tier), (tariff), ())
