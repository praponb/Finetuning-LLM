WITH scoped_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix,
        ADD_MONTHS(claim_date, 6) AS claim_date_plus_6m,
        CONCAT(claim_type, '#', CAST(policy_id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE claim_type = 'fire'
      AND approved_amount < 23207.7
),
with_parent AS (
    -- Attach adjuster attributes
    SELECT
        s.*,
        a.adjuster_name,
        p.premium AS policy_premium,
        p.status AS policy_status
    FROM scoped_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
enriched AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.status_prefix,
        s.claim_type_ref,
        s.adjuster_name,
        p.holder_id AS policy_holder_id,
        a.team AS adjuster_team
    FROM with_parent s
    JOIN policies p ON p.id = s.policy_id
    JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    adjuster_team,
    policy_holder_id,
    adjuster_id,
    COUNT(DISTINCT policy_holder_id) AS distinct_policy_holder_ids,
    DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days
FROM enriched
GROUP BY CUBE(adjuster_team, policy_holder_id, adjuster_id)
ORDER BY adjuster_team NULLS LAST, policy_holder_id NULLS LAST, adjuster_id NULLS LAST
