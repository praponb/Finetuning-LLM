WITH base AS (
    -- Filter tariff_windows
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        ROUND(rate * 0.25, 2) AS rate_adj,
        ROUND(rate * 1.1, 2) AS rate_adj_2,
        LOWER(TRIM(tariff)) AS tariff_clean
    FROM tariff_windows
    WHERE tariff <> 'FLAT'
      AND valid_from < TIMESTAMP '2025-03-09 04:00:00'
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        tariff_clean,
        ABS(rate_adj - 9.0) AS rate_adj_distance,
        rate + rate_adj_2 AS rate_plus_rate_adj_2
    FROM base
    WHERE UPPER(TRIM(tariff_clean)) LIKE 'F%'
),
stats AS (
    -- Summarise per group
    SELECT
        CASE WHEN rate_plus_rate_adj_2 >= 11.9 THEN 'large' WHEN rate_plus_rate_adj_2 >= 7.4 THEN 'medium' ELSE 'small' END AS rate_plus_rate_adj_2_tier,
        DATE_FORMAT(valid_from, 'yyyy-MM') AS valid_from_ym,
        CASE WHEN rate >= 6.1 THEN 'A' WHEN rate >= 3.7 THEN 'B' ELSE 'C' END AS rate_tier,
        MAX(rate_plus_rate_adj_2) AS max_rate_plus_rate_adj_2,
        ROUND(AVG(rate), 2) AS avg_rate,
        ROUND(STDDEV(rate_plus_rate_adj_2), 2) AS stddev_rate_plus_rate_adj_2,
        MAX(valid_from) AS last_valid_from
    FROM enriched
    GROUP BY 1, 2, 3
    HAVING MAX(rate) > 4.2
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM stats
    WHERE avg_rate < (SELECT MAX(avg_rate) FROM stats)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE stddev_rate_plus_rate_adj_2 > (SELECT AVG(stddev_rate_plus_rate_adj_2) FROM qualified)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT last_valid_from, rate_plus_rate_adj_2_tier, rate_tier, stddev_rate_plus_rate_adj_2
    FROM without_matches
) src
PIVOT (
    COUNT(stddev_rate_plus_rate_adj_2)
    FOR rate_tier IN ('A', 'C')
)
