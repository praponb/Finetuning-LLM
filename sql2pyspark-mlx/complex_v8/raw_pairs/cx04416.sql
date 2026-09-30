WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE (MONTH(valid_from) IN (1, 3) OR rate <= 5.7)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        valid_from,
        valid_to,
        CASE WHEN rate >= 6.1 THEN 'HIGH' WHEN rate >= 3.0 THEN 'MEDIUM' ELSE 'LOW' END AS rate_tier,
        COALESCE(rate, 0.0) AS rate_filled,
        ROUND(rate * 1.1, 2) AS rate_adj
    FROM filtered
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(rate_filled) OVER (PARTITION BY YEAR(valid_from), rate_tier) AS valid_from_year_rate_tier_max_rate_filled,
        ROUND(CUME_DIST() OVER (ORDER BY rate_filled DESC), 3) AS rate_filled_cume_dist,
        SUM(rate_filled) OVER (ORDER BY rate_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rate_filled
    FROM scored
),
filtered_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM ranked
    WHERE rate_filled > (SELECT MIN(rate_filled) FROM ranked)
)
SELECT
    id,
    valid_from,
    valid_to,
    rate_tier,
    rate_filled,
    rate_adj,
    valid_from_year_rate_tier_max_rate_filled,
    rate_filled_cume_dist,
    running_rate_filled,
    rate_adj - rate_filled AS rate_adj_minus_rate_filled
FROM filtered_2
WHERE rate_adj >= 5.0
ORDER BY rate_adj DESC, id
LIMIT 20
