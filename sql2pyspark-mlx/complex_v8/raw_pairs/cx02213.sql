WITH recent_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        LENGTH(TRIM(tariff)) AS tariff_len,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug_2
    FROM tariff_windows
    WHERE tariff NOT IN ('EV', 'FLAT')
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM recent_tariffs
    WHERE rate > (SELECT AVG(rate) FROM recent_tariffs)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(valid_from), tariff ORDER BY rate), 3) AS rate_percent_rank,
        DENSE_RANK() OVER (PARTITION BY MONTH(valid_from), tariff ORDER BY rate, id) AS rate_dense_rank
    FROM screened
),
tariff_stats AS (
    -- Avg rate_percent_rank per tariff
    SELECT tariff, AVG(rate_percent_rank) AS avg_rate_percent_rank
    FROM metrics
    GROUP BY tariff
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_rate_percent_rank,
        ROUND(s.rate_percent_rank - g.avg_rate_percent_rank, 2) AS rate_percent_rank_minus_avg
    FROM metrics s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.rate_percent_rank > g.avg_rate_percent_rank
)
-- Subtotals with GROUPING SETS
SELECT
    rate_dense_rank,
    tariff_len,
    PERCENTILE_APPROX(rate_percent_rank_minus_avg, 0.5) AS median_rate_percent_rank_minus_avg,
    ARRAY_SORT(COLLECT_SET(tariff)) AS tariff_list
FROM above_average
GROUP BY GROUPING SETS ((rate_dense_rank, tariff_len), (rate_dense_rank), (tariff_len), ())
ORDER BY rate_dense_rank NULLS LAST, tariff_len NULLS LAST
