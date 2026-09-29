WITH scoped_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug,
        CASE WHEN tariff IN ('EV', 'TOU') THEN 'group_a' WHEN tariff = 'FLAT' THEN 'group_b' ELSE 'other' END AS tariff_group
    FROM tariff_windows
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        tariff,
        tariff_slug,
        SIZE(COLLECT_SET(tariff_group) OVER w_tariff) AS distinct_tariff_group_in_tariff,
        FIRST_VALUE(rate) OVER (PARTITION BY tariff_group ORDER BY rate DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_rate,
        ROW_NUMBER() OVER (PARTITION BY tariff_group, tariff ORDER BY rate DESC NULLS FIRST, id) AS rn,
        COUNT(*) OVER (PARTITION BY tariff) AS rows_in_tariff
    FROM scoped_tariffs
    WINDOW w_tariff AS (PARTITION BY tariff)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        tariff,
        rn,
        COUNT(*) AS cnt,
        ROUND(AVG(rows_in_tariff), 2) AS avg_rows_in_tariff,
        COUNT(DISTINCT tariff) AS distinct_tariffs,
        ARRAY_SORT(COLLECT_SET(tariff)) AS tariff_list
    FROM ranked
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
)
SELECT tariff, rn, metric, reading
FROM (
    SELECT tariff, rn, cnt, distinct_tariffs
    FROM rollup_base
) src
UNPIVOT (
    reading FOR metric IN (cnt AS cnt_v, distinct_tariffs AS distinct_tariffs_v)
) unp
ORDER BY tariff, rn, metric
