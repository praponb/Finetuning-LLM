WITH recent AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-02-06 07:00:00'
)
SELECT
    policy_id AS entity_id,
    approved_amount AS value,
    reported_at AS event_time,
    'claim' AS source
FROM claims
UNION ALL
SELECT
    policy_id,
    approved_amount,
    reported_at,
    'claim_2' AS source
FROM claims
WHERE claimed_amount <= 38909.4
  AND reported_at < TIMESTAMP '2025-03-08 14:00:00'
