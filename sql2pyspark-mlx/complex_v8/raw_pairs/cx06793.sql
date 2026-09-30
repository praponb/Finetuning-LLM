WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CASE WHEN approved_amount >= 15477.7 THEN 'A' WHEN approved_amount >= 0.0 THEN 'B' ELSE 'C' END AS approved_amount_tier
    FROM claims
    WHERE adjuster_id IS NOT NULL
)
SELECT policy_id, id AS entity_id, claimed_amount
FROM valid
WHERE adjuster_id IS NOT NULL
  AND claim_date < DATE '2024-12-22'
UNION ALL
SELECT id AS policy_id, adjuster_id AS entity_id, claimed_amount
FROM claims
ORDER BY policy_id
