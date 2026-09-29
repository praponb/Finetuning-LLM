WITH valid_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        CASE WHEN tariff IN ('TOU', 'EV') THEN 'group_a' WHEN tariff = 'FLAT' THEN 'group_b' ELSE 'other' END AS tariff_group
    FROM tariff_windows
    WHERE TO_DATE(valid_to) < DATE '2025-02-18'
      AND rate >= 3.0
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM valid_tariffs
    WHERE rate < (SELECT MAX(rate) FROM valid_tariffs)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(valid_from, 'yyyy-MM') AS valid_from_ym,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt,
        COUNT(DISTINCT tariff_group) AS distinct_tariff_groups,
        MAX(valid_to) AS last_valid_to
    FROM screened
    WHERE rate > 5.2
    GROUP BY DATE_FORMAT(valid_from, 'yyyy-MM')
    HAVING AVG(rate) > 3.7
)
SELECT valid_from_ym, metric, metric_value
FROM (
    SELECT valid_from_ym, distinct_ids, distinct_tariff_groups
    FROM rollup_base
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (distinct_ids AS distinct_ids_v, distinct_tariff_groups AS distinct_tariff_groups_v)
) unp
