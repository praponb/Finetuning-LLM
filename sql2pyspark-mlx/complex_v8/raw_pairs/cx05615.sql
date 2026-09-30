WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status,
        reported_at
    FROM claims
    WHERE adjuster_id IS NOT NULL
)
SELECT id
FROM cleaned_claims
EXCEPT
SELECT id
FROM policies
WHERE holder_id IS NOT NULL
ORDER BY id
