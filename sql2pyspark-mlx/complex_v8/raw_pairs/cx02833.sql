WITH filtered_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        valid_from + INTERVAL 1 DAYS AS valid_from_plus_1d,
        CASE WHEN tariff IN ('FLAT', 'EV') THEN 'group_a' WHEN tariff = 'TOU' THEN 'group_b' ELSE 'other' END AS tariff_group
    FROM tariff_windows
    WHERE rate <= 7.6
      AND valid_from >= TIMESTAMP '2025-01-23 19:00:00'
),
annotated AS (
    -- Add calculated columns
    SELECT *, SUBSTRING(TRIM(tariff_group), 1, 4) AS tariff_group_prefix
    FROM filtered_tariffs
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM annotated
    WHERE rate > (SELECT MIN(rate) FROM annotated)
)
-- Subtotals with GROUPING SETS
SELECT
    tariff_group,
    tariff,
    MIN(rate) AS min_rate
FROM filtered
GROUP BY GROUPING SETS ((tariff_group, tariff), (tariff_group))
