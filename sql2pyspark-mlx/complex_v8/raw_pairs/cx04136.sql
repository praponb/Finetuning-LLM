WITH valid_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment,
        COALESCE(risk_score, 0.0) AS risk_score_filled,
        COALESCE(risk_score, 0.0) AS risk_score_filled_2
    FROM policyholders
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        MIN(end_date) AS first_policy_end_date,
        MIN(end_date) AS first_policy_end_date_2
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.first_policy_end_date,
        p.first_policy_end_date_2
    FROM valid_holders s
    LEFT JOIN policy_stats p ON p.holder_id = s.id
)
SELECT
    segment,
    region,
    ROUND(SUM(risk_score_filled), 2) AS total_risk_score_filled,
    ROUND(SUM(risk_score_filled_2), 2) AS total_risk_score_filled_2,
    ARRAY_SORT(COLLECT_SET(region)) AS region_list
FROM summarized_rows
GROUP BY GROUPING SETS ((segment, region), (segment), ())
ORDER BY segment NULLS LAST, region NULLS LAST
