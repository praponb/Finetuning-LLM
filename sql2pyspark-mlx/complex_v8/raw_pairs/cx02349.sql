WITH cleaned_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        risk_score,
        joined_on,
        segment,
        CASE WHEN risk_score >= 0.6 THEN 'large' WHEN risk_score >= 0.2 THEN 'medium' ELSE 'small' END AS risk_score_tier,
        CASE WHEN risk_score >= 0.8 THEN 'HIGH' WHEN risk_score >= 0.2 THEN 'MEDIUM' ELSE 'LOW' END AS risk_score_tier_2,
        ROUND(risk_score * 1.1, 2) AS risk_score_adj
    FROM policyholders
    WHERE segment <> 'VIP'
      AND LENGTH(TRIM(holder_name)) >= 4
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MAX(p.start_date) FROM policies p WHERE p.holder_id = s.id) AS last_policy_start_date,
        (SELECT MAX(p.end_date) FROM policies p WHERE p.holder_id = s.id) AS last_policy_end_date
    FROM cleaned_holders s
),
enriched AS (
    -- Derive helper columns
    SELECT *, COALESCE(risk_score_tier, 'UNKNOWN') AS risk_score_tier_or_unknown
    FROM with_counts
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM enriched
    WHERE risk_score_adj > (SELECT AVG(risk_score_adj) FROM enriched)
)
-- Final projection
SELECT
    id,
    joined_on,
    segment,
    risk_score_tier_2,
    risk_score_adj,
    last_policy_start_date,
    last_policy_end_date,
    CASE WHEN risk_score_adj >= 1.2 THEN 'HIGH' WHEN risk_score_adj >= 0.6 THEN 'MEDIUM' ELSE 'LOW' END AS risk_score_adj_tier,
    CASE WHEN risk_score >= 0.7 THEN 'large' WHEN risk_score >= 0.2 THEN 'medium' ELSE 'small' END AS risk_score_tier_3
FROM qualified
ORDER BY last_policy_end_date DESC, id
LIMIT 10
