WITH filtered AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        ROUND(rate * 0.25, 2) AS rate_adj,
        valid_from + INTERVAL 15 MINUTES AS valid_from_plus_15m,
        CASE WHEN rate >= 5.7 THEN 'HIGH' WHEN rate >= 3.0 THEN 'MEDIUM' ELSE 'LOW' END AS rate_tier
    FROM tariff_windows
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(rate, 0.0) AS rate_filled,
        CASE WHEN rate >= 7.6 THEN 'HIGH' WHEN rate >= 3.0 THEN 'MEDIUM' ELSE 'LOW' END AS rate_tier_2,
        CASE WHEN rate_tier IN ('LOW', 'MEDIUM') THEN 'group_a' WHEN rate_tier = 'HIGH' THEN 'group_b' ELSE 'other' END AS rate_tier_group
    FROM filtered
),
tariff_stats AS (
    -- Avg rate_adj per tariff
    SELECT tariff, AVG(rate_adj) AS avg_rate_adj
    FROM scored
    GROUP BY tariff
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_rate_adj,
        ROUND(s.rate_adj - g.avg_rate_adj, 2) AS rate_adj_minus_avg
    FROM scored s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.rate_adj > g.avg_rate_adj
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT rate_tier_2, tariff, rate_tier_group, rate_adj
    FROM compared
) src
PIVOT (
    SUM(rate_adj) AS total, COUNT(rate_adj) AS n
    FOR rate_tier_group IN ('other', 'group_b', 'group_a')
)
ORDER BY rate_tier_2, tariff
