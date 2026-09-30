WITH filtered_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment
    FROM policyholders
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        DATEDIFF(joined_on, LAG(joined_on) OVER (PARTITION BY segment ORDER BY joined_on, id)) AS days_since_prev_joined_on,
        LAST_VALUE(joined_on) IGNORE NULLS OVER (ORDER BY joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_joined_on,
        RANK() OVER (PARTITION BY segment ORDER BY joined_on) AS joined_on_rank,
        NTH_VALUE(joined_on, 2) OVER (PARTITION BY segment ORDER BY joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_joined_on,
        FIRST_VALUE(joined_on) IGNORE NULLS OVER (PARTITION BY segment ORDER BY joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_joined_on
    FROM filtered_holders
)
SELECT
    COALESCE(segment, 'ALL') AS segment,
    joined_on_rank,
    COALESCE(region, 'ALL') AS region,
    PERCENTILE_APPROX(joined_on_rank, 0.5) AS median_joined_on_rank,
    COUNT(*) AS cnt,
    MAX(second_joined_on) AS last_second_joined_on
FROM metrics
GROUP BY GROUPING SETS ((segment, joined_on_rank, region), (segment), (joined_on_rank), ())
ORDER BY segment NULLS LAST, joined_on_rank NULLS LAST, region NULLS LAST
