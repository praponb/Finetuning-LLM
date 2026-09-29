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
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount
    FROM claims
    WHERE claim_type = 'fire'
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.least_approved_amount_claimed_amount,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.status AS claim_status,
        c.claim_type AS claim_claim_type
    FROM valid_claims s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.least_approved_amount_claimed_amount,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_status,
        s.claim_claim_type,
        c.status AS claim_status_2
    FROM exploded_detail_claims s
    JOIN claims c ON c.id = s.claim_id
    WHERE c.status IN ('APPROVED', 'OPEN', 'PAID')
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount
    FROM joined
),
grouped AS (
    -- Summarise per group
    SELECT
        policy_id,
        claim_adjuster_id,
        ROUND(SUM(claimed_amount) / NULLIF(SUM(approved_amount), 0), 3) AS claimed_amount_to_approved_amount_ratio,
        COUNT(DISTINCT claim_adjuster_id) AS distinct_claim_adjuster_ids,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE claim_status_2 NOT IN ('REJECTED', 'IN_REVIEW')) AS n_claim_status_2_not_in_reject,
        COUNT(DISTINCT claim_claim_type) AS distinct_claim_claim_types
    FROM tagged
    WHERE least_approved_amount_claimed_amount <= 40000.0
    GROUP BY 1, 2
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM grouped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.claim_adjuster_id
              AND c.approved_amount >= 2887.3
              AND c.reported_at >= TIMESTAMP '2025-01-19 14:00:00'
        )
)
SELECT
    policy_id,
    claim_adjuster_id,
    claimed_amount_to_approved_amount_ratio,
    distinct_claim_adjuster_ids,
    cnt,
    n_claim_status_2_not_in_reject,
    distinct_claim_claim_types
FROM screened
ORDER BY claimed_amount_to_approved_amount_ratio DESC, n_claim_status_2_not_in_reject DESC, policy_id, claim_adjuster_id
LIMIT 5
