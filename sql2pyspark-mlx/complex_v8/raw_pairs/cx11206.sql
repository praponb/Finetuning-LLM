WITH prepared_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug
    FROM tariff_windows
    WHERE valid_from < TIMESTAMP '2025-02-18 04:00:00'
),
derived AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(tariff), 1, 3) AS tariff_prefix
    FROM prepared_tariffs
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        tariff,
        valid_to,
        tariff_prefix,
        LEAD(valid_to) OVER (ORDER BY valid_from, id) AS next_valid_to,
        ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY valid_from, id) AS seq_no,
        LAST_VALUE(valid_to) OVER (ORDER BY valid_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_valid_to
    FROM derived
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown_2
    FROM with_windows
),
rollup_base AS (
    -- Summarise per group
    SELECT
        seq_no,
        tariff,
        tariff_or_unknown,
        SUM(CASE WHEN tariff IN ('TOU', 'FLAT') THEN 1 ELSE 0 END) AS n_tariff_in_tou_flat,
        SUM(CASE WHEN tariff <> 'FLAT' THEN seq_no ELSE 0 END) AS seq_no_tariff_flat,
        ARRAY_SORT(COLLECT_SET(tariff)) AS tariff_list,
        COUNT(*) AS n_rows
    FROM scored
    GROUP BY seq_no, tariff, tariff_or_unknown
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE n_tariff_in_tou_flat > (SELECT AVG(n_tariff_in_tou_flat) FROM rollup_base)
)
SELECT
    seq_no,
    tariff,
    tariff_or_unknown,
    n_tariff_in_tou_flat,
    seq_no_tariff_flat,
    tariff_list,
    n_rows,
    INITCAP(TRIM(tariff_or_unknown)) AS tariff_or_unknown_clean
FROM qualified
ORDER BY tariff DESC NULLS FIRST
