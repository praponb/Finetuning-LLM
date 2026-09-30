WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE approved_amount > 2887.3
),
with_parent AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        a.adjuster_name,
        a.team AS adjuster_team,
        a.level AS adjuster_level
    FROM filtered_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id AND LENGTH(TRIM(a.adjuster_name)) >= 5
),
grouped AS (
    -- Summarise per group
    SELECT
        policy_id,
        adjuster_id,
        PERCENTILE_APPROX(approved_amount, 0.5) AS median_approved_amount,
        SUM(adjuster_level) FILTER (WHERE adjuster_team IS NOT NULL) AS adjuster_level_adjuster_team_is_not_null,
        COUNT(DISTINCT claim_type) AS distinct_claim_types
    FROM with_parent
    GROUP BY 1, 2
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(median_approved_amount, 0.0) AS median_approved_amount_filled,
        FLOOR(median_approved_amount / 10000) * 10000 AS median_approved_amount_bucket,
        distinct_claim_types - median_approved_amount AS distinct_claim_types_minus_median_approved_amoun
    FROM grouped
    WHERE policy_id >= 25
)
-- Native SQL UNPIVOT (wide to long)
SELECT policy_id, adjuster_id, metric, value
FROM (
    SELECT policy_id, adjuster_id, median_approved_amount_bucket, adjuster_level_adjuster_team_is_not_null
    FROM annotated
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (median_approved_amount_bucket AS median_approved_amount_bucket_v, adjuster_level_adjuster_team_is_not_null AS m_adjuster_level_adjuster_team_is_not_null)
) unp
ORDER BY policy_id, adjuster_id, metric
