WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at
    FROM claims
    WHERE adjuster_id IS NOT NULL
)
SELECT id AS qty, reported_at AS happened_at, 'claim' AS source
FROM filtered
UNION
SELECT policy_id, reported_at, 'claim_2' AS source
FROM claims
ORDER BY happened_at
