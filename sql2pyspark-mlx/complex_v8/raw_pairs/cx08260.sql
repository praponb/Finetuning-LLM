WITH valid_claims AS (
    -- Keep valid rows and derive helper columns
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
    WHERE YEAR(claim_date) = 2025
)
SELECT id AS key_id, claim_type
FROM valid_claims
WHERE status = 'OPEN'
  AND policy_id IS NOT NULL
EXCEPT ALL
SELECT id AS key_id, status AS claim_type
FROM claims
ORDER BY claim_type, key_id
