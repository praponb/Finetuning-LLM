WITH valid_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_to,
        rate
    FROM tariff_windows
    WHERE rate BETWEEN 3.7 AND 6.1
      AND valid_to < TIMESTAMP '2025-03-09 04:00:00'
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM valid_tariffs
    WHERE rate < (SELECT MAX(rate) FROM valid_tariffs)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        tariff,
        valid_to,
        rate,
        ROUND(PERCENT_RANK() OVER (PARTITION BY tariff ORDER BY valid_to DESC), 3) AS valid_to_percent_rank,
        ROUND(rate - AVG(rate) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_to), tariff), 2) AS rate_vs_valid_to_month_tariff_avg,
        MIN(rate) OVER w_valid_to_month_tariff AS valid_to_month_tariff_min_rate,
        ROUND(CUME_DIST() OVER (PARTITION BY tariff ORDER BY rate, id), 3) AS rate_cume_dist
    FROM without_matches
    WINDOW w_valid_to_month_tariff AS (PARTITION BY DATE_TRUNC('MONTH', valid_to), tariff)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT valid_to, tariff, rate_vs_valid_to_month_tariff_avg
    FROM sequenced
) src
PIVOT (
    MIN(rate_vs_valid_to_month_tariff_avg)
    FOR tariff IN ('FLAT', 'TOU')
)
ORDER BY valid_to
