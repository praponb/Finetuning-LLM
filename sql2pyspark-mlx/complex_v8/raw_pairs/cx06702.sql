WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment
    FROM policyholders
    WHERE joined_on < DATE '2025-02-22'
      AND region = 'West'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(joined_on) OVER (PARTITION BY segment, DATE_TRUNC('MONTH', joined_on) ORDER BY joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_joined_on,
        NTILE(4) OVER (PARTITION BY segment, DATE_TRUNC('MONTH', joined_on) ORDER BY joined_on, id) AS joined_on_quartile,
        LAST_VALUE(joined_on) IGNORE NULLS OVER (PARTITION BY segment, DATE_TRUNC('MONTH', joined_on) ORDER BY joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_joined_on
    FROM filtered
)
-- Subtotals with CUBE
SELECT
    COALESCE(segment, 'ALL') AS segment,
    COALESCE(region, 'ALL') AS region,
    joined_on_quartile,
    GROUPING(segment) AS is_total_segment,
    ROUND(STDDEV(joined_on_quartile), 2) AS stddev_joined_on_quartile
FROM metrics
GROUP BY CUBE(segment, region, joined_on_quartile)
ORDER BY segment NULLS LAST, region NULLS LAST, joined_on_quartile NULLS LAST
