WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE claimed_amount <= 41760.8
      AND policy_id IS NOT NULL
)
SELECT adjuster_id AS entity_id, status AS name
FROM recent_claims
UNION ALL
SELECT adjuster_id, status
FROM claims
WHERE claimed_amount BETWEEN 4234.9 AND 30001.6
  AND claim_date >= DATE '2024-06-07'
