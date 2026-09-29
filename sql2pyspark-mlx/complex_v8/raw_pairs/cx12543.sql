WITH base AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        CASE WHEN tariff IN ('FLAT', 'EV') THEN 'group_a' WHEN tariff = 'TOU' THEN 'group_b' ELSE 'other' END AS tariff_group,
        CASE WHEN installed_kw >= 386.6 THEN 'A' WHEN installed_kw >= 96.9 THEN 'B' ELSE 'C' END AS installed_kw_tier,
        DATE_FORMAT(commissioned_on, 'yyyy-MM-dd') AS commissioned_on_label
    FROM sites
    WHERE tariff = 'TOU'
      AND commissioned_on IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_name,
        tariff,
        tariff_group,
        installed_kw_tier,
        commissioned_on_label,
        LEAD(installed_kw) OVER (PARTITION BY zone ORDER BY commissioned_on NULLS LAST, id) AS next_installed_kw,
        SUM(installed_kw) OVER w_tariff_group_tariff AS tariff_group_tariff_sum_installed_kw,
        ROUND(CUME_DIST() OVER (PARTITION BY zone ORDER BY commissioned_on NULLS LAST), 3) AS commissioned_on_cume_dist,
        DATEDIFF(commissioned_on, LAG(commissioned_on) OVER (PARTITION BY zone ORDER BY commissioned_on NULLS LAST, id)) AS days_since_prev_commissioned_on
    FROM base
    WINDOW w_tariff_group_tariff AS (PARTITION BY tariff_group, tariff)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        site_name,
        tariff,
        tariff_group,
        installed_kw_tier,
        commissioned_on_label,
        tariff_group_tariff_sum_installed_kw,
        commissioned_on_cume_dist,
        days_since_prev_commissioned_on,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        LENGTH(TRIM(tariff_group)) AS tariff_group_len,
        LOWER(REGEXP_REPLACE(TRIM(site_name), ' ', '_')) AS site_name_slug
    FROM ranked
)
-- Subtotals with GROUPING SETS
SELECT
    tariff_or_unknown,
    tariff,
    ARRAY_SORT(COLLECT_SET(site_name)) AS site_name_list,
    ARRAY_SORT(COLLECT_SET(tariff)) AS tariff_list
FROM enriched
GROUP BY GROUPING SETS ((tariff_or_unknown, tariff), (tariff_or_unknown), (tariff))
ORDER BY tariff_or_unknown NULLS LAST, tariff NULLS LAST
