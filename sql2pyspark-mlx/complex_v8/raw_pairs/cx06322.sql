WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at + INTERVAL 30 MINUTES AS reported_at_plus_30m,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount
    FROM claims
    WHERE claim_date >= DATE '2024-04-11'
      AND approved_amount BETWEEN 2272.7 AND 32511.8
),
with_parent_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.approved_amount_per_claimed_amount,
        p.status AS policy_status,
        p.end_date AS policy_end_date,
        p.product AS policy_product
    FROM filtered s
    JOIN policies p ON p.id = s.policy_id
),
filtered_2 AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_parent_policy s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claimed_amount < 44991.5
        )
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.approved_amount_per_claimed_amount,
        s.policy_status,
        s.policy_end_date,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.claim_type,
        c.claimed_amount AS claim_claimed_amount
    FROM filtered_2 s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(claim_claimed_amount / NULLIF(claimed_amount, 0), 3) AS claim_claimed_amount_per_claimed_amount
    FROM with_children
),
enriched_claim AS (
    -- Join claims
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.policy_status,
        s.policy_end_date,
        s.claim_id,
        s.claim_policy_id,
        s.claim_type,
        s.claim_claimed_amount_per_claimed_amount,
        c.policy_id AS claim_policy_id_2
    FROM scored s
    JOIN claims c ON c.id = s.claim_id
    WHERE c.policy_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    claim_date,
    claimed_amount,
    status,
    policy_status,
    claim_id,
    claim_policy_id,
    claim_type,
    claim_claimed_amount_per_claimed_amount,
    claim_policy_id_2
FROM enriched_claim
WHERE claimed_amount BETWEEN 17759.3 AND 33975.8
ORDER BY status DESC
