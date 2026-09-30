WITH scoped AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        ABS(rate - 4.2) AS rate_distance,
        CASE WHEN rate >= 5.7 THEN 'A' WHEN rate >= 3.7 THEN 'B' ELSE 'C' END AS rate_tier
    FROM tariff_windows
    WHERE tariff <> 'FLAT'
      AND rate < 7.6
),
tariff_stats AS (
    -- Avg rate per tariff
    SELECT tariff, AVG(rate) AS avg_rate
    FROM scoped
    GROUP BY tariff
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_rate, ROUND(s.rate - g.avg_rate, 2) AS rate_minus_avg
    FROM scoped s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.rate > g.avg_rate
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(rate_tier, 'ALL') AS rate_tier,
    COALESCE(tariff, 'ALL') AS tariff,
    GROUPING(rate_tier) AS is_total_rate_tier,
    PERCENTILE_APPROX(avg_rate, 0.5) AS median_avg_rate,
    COUNT(*) AS row_count
FROM compared
GROUP BY GROUPING SETS ((rate_tier, tariff), (rate_tier), ())
ORDER BY rate_tier NULLS LAST, tariff NULLS LAST
