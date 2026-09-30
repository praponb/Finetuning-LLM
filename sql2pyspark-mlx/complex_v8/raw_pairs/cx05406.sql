WITH scoped_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        segment
    FROM policyholders
    WHERE risk_score <= 0.6
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        region,
        segment,
        ROUND(AVG(risk_score), 2) AS avg_risk_score,
        COUNT(DISTINCT region) AS distinct_regions
    FROM scoped_holders
    GROUP BY region, segment
    HAVING MAX(risk_score) > 0.3
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        region,
        segment,
        distinct_regions,
        LEAD(avg_risk_score) OVER w_distinct_regions_region_by_d AS next_avg_risk_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_regions, region ORDER BY distinct_regions DESC), 3) AS distinct_regions_percent_rank
    FROM totals
    WINDOW w_distinct_regions_region_by_d AS (PARTITION BY distinct_regions, region ORDER BY distinct_regions DESC, region, segment)
)
SELECT
    region,
    segment,
    distinct_regions,
    next_avg_risk_score,
    LOWER(REGEXP_REPLACE(TRIM(region), ' ', '_')) AS region_slug
FROM with_trends
WHERE next_avg_risk_score > 0.4
ORDER BY region_slug DESC NULLS FIRST, region NULLS LAST
