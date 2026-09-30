WITH filtered AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY zone, tariff) AS rows_in_zone_tariff,
        FIRST_VALUE(commissioned_on) OVER (PARTITION BY tariff ORDER BY installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_commissioned_on,
        LAG(commissioned_on, 3) OVER (PARTITION BY tariff ORDER BY installed_kw, id) AS prev3_commissioned_on,
        MAX(installed_kw) OVER (ORDER BY installed_kw DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS installed_kw_max_1_prior,
        MIN(installed_kw) OVER (PARTITION BY zone, tariff) AS zone_tariff_min_installed_kw
    FROM filtered
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        first_commissioned_on,
        prev3_commissioned_on,
        installed_kw_max_1_prior,
        zone_tariff_min_installed_kw,
        CONCAT_WS(' / ', zone, site_name) AS zone_site_name_label,
        CONCAT(zone, '#', CAST(rows_in_zone_tariff AS STRING)) AS zone_ref,
        CASE WHEN zone IN ('Z2', 'Z4') THEN 'group_a' WHEN zone = 'Z3' THEN 'group_b' ELSE 'other' END AS zone_group
    FROM sequenced
    WHERE prev3_commissioned_on >= DATE '2024-08-24'
)
-- Subtotals with ROLLUP
SELECT
    zone,
    zone_group,
    tariff,
    SUM(installed_kw) AS total_installed_kw,
    MAX(first_commissioned_on) AS last_first_commissioned_on
FROM derived
GROUP BY ROLLUP(zone, zone_group, tariff)
