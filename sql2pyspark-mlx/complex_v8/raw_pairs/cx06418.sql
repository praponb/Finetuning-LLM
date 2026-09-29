WITH scoped AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        CASE WHEN rate >= 5.7 THEN 'HIGH' WHEN rate >= 3.0 THEN 'MEDIUM' ELSE 'LOW' END AS rate_tier,
        CASE WHEN rate >= 5.5 THEN 'HIGH' WHEN rate >= 4.2 THEN 'MEDIUM' ELSE 'LOW' END AS rate_tier_2
    FROM tariff_windows
    WHERE valid_from < TIMESTAMP '2025-03-09 04:00:00'
      AND valid_to >= TIMESTAMP '2025-01-25 21:00:00'
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        tariff,
        rate,
        rate_tier,
        CONCAT(rate_tier, '#', CAST(id AS STRING)) AS rate_tier_ref,
        MONTH(valid_from) AS valid_from_month
    FROM scoped
),
rollup_base AS (
    -- Group-level totals
    SELECT
        valid_from_month,
        CASE WHEN rate >= 5.9 THEN 'HIGH' WHEN rate >= 3.0 THEN 'MEDIUM' ELSE 'LOW' END AS rate_tier_2,
        rate_tier,
        SUM(valid_from_month) AS total_valid_from_month,
        SUM(rate) FILTER (WHERE tariff IS NOT NULL) AS rate_tariff_is_not_null,
        ARRAY_SORT(COLLECT_SET(tariff)) AS tariff_list,
        PERCENTILE_APPROX(valid_from_month, 0.5) AS median_valid_from_month
    FROM tagged
    GROUP BY valid_from_month, CASE WHEN rate >= 5.9 THEN 'HIGH' WHEN rate >= 3.0 THEN 'MEDIUM' ELSE 'LOW' END, rate_tier
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT median_valid_from_month, rate_tier_2, total_valid_from_month
    FROM rollup_base
) src
PIVOT (
    MIN(total_valid_from_month) AS min_v, SUM(total_valid_from_month) AS sum_v
    FOR rate_tier_2 IN ('LOW', 'MEDIUM')
)
ORDER BY median_valid_from_month
