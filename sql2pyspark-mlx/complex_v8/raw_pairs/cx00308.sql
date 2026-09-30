WITH filtered AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        rate
    FROM tariff_windows
    WHERE MONTH(valid_to) IN (2, 3)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(rate) OVER (PARTITION BY tariff), 2) AS tariff_avg_rate,
        ROUND(CUME_DIST() OVER (PARTITION BY tariff ORDER BY rate), 3) AS rate_cume_dist,
        SUM(rate) OVER (PARTITION BY tariff) AS tariff_sum_rate
    FROM filtered
    WINDOW w_tariff AS (PARTITION BY tariff)
),
per_group AS (
    -- Group-level totals
    SELECT
        CASE WHEN rate >= 5.7 THEN 'HIGH' WHEN rate >= 4.2 THEN 'MEDIUM' ELSE 'LOW' END AS rate_tier,
        DATE_TRUNC('MONTH', valid_from) AS valid_from_month,
        tariff,
        COUNT(*) AS n_rows,
        MIN(valid_from) AS first_valid_from,
        SUM(CASE WHEN tariff IN ('TOU', 'FLAT') THEN 1 ELSE 0 END) AS n_tariff_in_tou_flat,
        COUNT(DISTINCT tariff) AS distinct_tariffs,
        MAX(rate) AS max_rate
    FROM metrics
    GROUP BY 1, 2, 3
    HAVING AVG(tariff_avg_rate) > 4.1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT distinct_tariffs, n_tariff_in_tou_flat, tariff, n_rows
    FROM per_group
) src
PIVOT (
    AVG(n_rows)
    FOR tariff IN ('EV', 'TOU', 'FLAT')
)
ORDER BY distinct_tariffs, n_tariff_in_tou_flat
