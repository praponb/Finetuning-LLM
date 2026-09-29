WITH prepared AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status
    FROM claims
    WHERE status <> 'APPROVED'
      AND approved_amount >= 9389.3
)
SELECT status AS category, claim_date AS event_date
FROM prepared
UNION ALL
SELECT status AS category, claim_date AS event_date
FROM claims
WHERE claim_type IN ('medical', 'theft', 'fire')
  AND claim_date >= DATE '2024-04-07'
ORDER BY category, event_date
