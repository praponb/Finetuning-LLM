WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        CASE WHEN claim_type IN ('flood', 'fire') THEN 'group_a' WHEN claim_type = 'medical' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
    WHERE claim_type = 'theft'
      AND approved_amount > 0.0
),
combined_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claim_type_len,
        s.claim_type_group,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM filtered_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.team = 'Health'
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, approved_amount, claimed_amount
    FROM combined_details_adjuster
) src
UNPIVOT (
    metric_value FOR field IN (approved_amount, claimed_amount)
) unp
ORDER BY id, field
