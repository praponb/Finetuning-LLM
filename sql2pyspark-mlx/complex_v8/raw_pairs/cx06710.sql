WITH valid_claims AS (
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
        ABS(claimed_amount - 17759.3) AS claimed_amount_distance,
        LEAST(claimed_amount, approved_amount) AS least_claimed_amount_approved_amount
    FROM claims
    WHERE policy_id <= 29
      AND claim_type = 'theft'
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM valid_claims s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.status <> 'REJECTED'
              AND c.claim_type = 'flood'
        )
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.status,
        s.reported_at,
        s.claimed_amount_distance,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_type AS claim_claim_type
    FROM without_matches s
    JOIN claims c ON c.policy_id = s.policy_id AND c.adjuster_id <= 4
)
SELECT
    id,
    claim_date,
    status,
    reported_at,
    claimed_amount_distance,
    claim_claim_type
FROM detail_rows
ORDER BY reported_at DESC NULLS FIRST, id NULLS LAST
