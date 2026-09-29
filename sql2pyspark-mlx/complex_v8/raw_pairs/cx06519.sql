WITH cleaned_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        ABS(risk_score - 0.2) AS risk_score_distance,
        COALESCE(region, 'UNKNOWN') AS region_or_unknown
    FROM policyholders
    WHERE LENGTH(TRIM(holder_name)) >= 7
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        SUM(premium) AS total_policy_premium,
        SUM(premium) AS total_policy_premium_2
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.total_policy_premium,
        p.total_policy_premium_2
    FROM cleaned_holders s
    LEFT JOIN policy_stats p ON p.holder_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    segment,
    region,
    region_or_unknown,
    COUNT(*) AS cnt,
    SUM(total_policy_premium_2) AS total_total_policy_premium_2,
    COUNT(DISTINCT id) AS distinct_ids
FROM summarized_rows
GROUP BY GROUPING SETS ((segment, region, region_or_unknown), (segment), (region), ())
ORDER BY segment NULLS LAST, region NULLS LAST, region_or_unknown NULLS LAST
