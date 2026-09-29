WITH valid_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        MONTH(commissioned_on) AS commissioned_on_month,
        ROUND(installed_kw * 1.07, 2) AS installed_kw_adj,
        DATEDIFF(DATE '2025-06-30', commissioned_on) AS days_since_commissioned_on
    FROM sites
    WHERE tariff = 'FLAT'
      AND LOWER(site_name) LIKE '%la%'
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        commissioned_on_month,
        installed_kw_adj,
        days_since_commissioned_on,
        CASE WHEN installed_kw >= 284.0 THEN 'HIGH' WHEN installed_kw >= 141.6 THEN 'MEDIUM' ELSE 'LOW' END AS installed_kw_tier,
        DATE_TRUNC('MONTH', commissioned_on) AS commissioned_on_month_2,
        DATE_FORMAT(commissioned_on, 'MMM yyyy') AS commissioned_on_label
    FROM valid_sites
    WHERE commissioned_on < DATE '2024-11-21'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT commissioned_on, installed_kw_tier, tariff, installed_kw_adj
    FROM labeled
) src
PIVOT (
    MIN(installed_kw_adj)
    FOR tariff IN ('TOU', 'EV', 'FLAT')
)
ORDER BY commissioned_on, installed_kw_tier
