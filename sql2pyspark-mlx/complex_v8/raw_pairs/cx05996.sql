WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM tariff_windows
    WHERE tariff IN ('TOU', 'FLAT')
      AND rate <= 6.1
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        tariff,
        rate,
        tariff_ref,
        ABS(rate - 3.7) AS rate_distance,
        UPPER(TRIM(tariff)) AS tariff_clean,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown_2
    FROM filtered
),
totals AS (
    -- Summarise per group
    SELECT
        tariff_or_unknown_2,
        tariff,
        ARRAY_SORT(COLLECT_SET(tariff)) AS tariff_list
    FROM enriched
    GROUP BY tariff_or_unknown_2, tariff
),
labeled AS (
    -- Derive helper columns
    SELECT
        tariff_or_unknown_2,
        tariff,
        tariff_list,
        ARRAY_JOIN(ARRAY_SORT(tariff_list), ',') AS tariff_list_text,
        SUBSTRING(TRIM(tariff_or_unknown_2), 1, 4) AS tariff_or_unknown_2_prefix,
        CASE WHEN tariff_or_unknown_2 IN ('TOU', 'FLAT') THEN 'group_a' WHEN tariff_or_unknown_2 = 'EV' THEN 'group_b' ELSE 'other' END AS tariff_or_unknown_2_group
    FROM totals
),
exploded AS (
    -- One row per tariff_list_item
    SELECT
        s.tariff_or_unknown_2,
        s.tariff,
        s.tariff_list_text,
        s.tariff_or_unknown_2_prefix,
        s.tariff_or_unknown_2_group,
        t.tariff_list_item
    FROM labeled s
    LATERAL VIEW OUTER EXPLODE(s.tariff_list) t AS tariff_list_item
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(tariff_list_item, 'ALL') AS tariff_list_item,
    COALESCE(tariff_or_unknown_2, 'ALL') AS tariff_or_unknown_2,
    COUNT(*) AS row_count,
    SUM(CASE WHEN tariff NOT IN ('TOU', 'EV') THEN 1 ELSE 0 END) AS n_tariff_not_in_tou_ev
FROM exploded
GROUP BY GROUPING SETS ((tariff_list_item, tariff_or_unknown_2), (tariff_list_item))
