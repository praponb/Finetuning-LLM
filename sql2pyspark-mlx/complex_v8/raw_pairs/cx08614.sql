WITH scoped_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status
    FROM claims
    WHERE adjuster_id >= 3
      AND status NOT IN ('IN_REVIEW', 'OPEN')
)
SELECT policy_id
FROM scoped_claims
WHERE status IN ('PAID', 'REJECTED', 'IN_REVIEW')
  AND adjuster_id IS NOT NULL
INTERSECT ALL
SELECT id
FROM policyholders
WHERE LENGTH(TRIM(holder_name)) >= 5
  AND region = 'East'
ORDER BY policy_id DESC
