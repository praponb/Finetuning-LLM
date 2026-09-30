WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        CASE WHEN claim_type IN ('fire', 'collision') THEN 'group_a' WHEN claim_type = 'medical' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
    WHERE adjuster_id >= 6
)
SELECT
    'claim' AS src,
    policy_id AS entity_id,
    claim_type_group,
    claim_date
FROM filtered_claims
WHERE adjuster_id IS NOT NULL
  AND status IS NOT NULL
UNION ALL
SELECT
    'claim_2' AS src,
    id AS entity_id,
    status AS claim_type_group,
    claim_date
FROM claims
UNION ALL
SELECT
    'policy' AS src,
    id AS entity_id,
    status AS claim_type_group,
    end_date AS claim_date
FROM policies
WHERE premium > 2111.3
ORDER BY src DESC, claim_type_group
