WITH cleaned_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status,
        reported_at
    FROM claims
    WHERE claim_date BETWEEN DATE '2024-06-13' AND DATE '2025-10-29'
)
SELECT policy_id AS key_id, reported_at AS event_time
FROM cleaned_claims
UNION ALL
SELECT adjuster_id, reported_at
FROM claims
WHERE claim_type NOT IN ('theft', 'collision')
  AND claimed_amount >= 19347.2
ORDER BY event_time DESC, key_id
