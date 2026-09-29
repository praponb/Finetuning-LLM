WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at,
        DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label
    FROM claims
    WHERE claimed_amount <= 32796.4
      AND TO_DATE(reported_at) < DATE '2025-03-14'
),
joined_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.reported_at,
        s.claim_date_label,
        p.premium AS policy_premium
    FROM recent_claims s
    JOIN policies p ON p.id = s.policy_id AND p.status = 'EXPIRED'
),
latest AS (
    -- Keep the latest row per claim_type, adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at,
        claim_date_label,
        policy_premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type, adjuster_id ORDER BY reported_at DESC, id DESC) AS recency_rank
        FROM joined_policy
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    adjuster_id,
    claim_date,
    claim_type,
    reported_at,
    policy_premium
FROM latest
ORDER BY claim_date DESC, policy_premium DESC, id
LIMIT 20
