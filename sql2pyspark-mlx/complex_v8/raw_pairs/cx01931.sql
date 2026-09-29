WITH prepared_claims AS (
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
        reported_at
    FROM claims
    WHERE claim_type = 'flood'
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS first_claim_date,
        (SELECT SUM(c.approved_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS total_claim_approved_amount,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS first_claim_reported_at
    FROM prepared_claims s
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_stats
    WHERE total_claim_approved_amount > (SELECT AVG(total_claim_approved_amount) FROM with_stats)
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.policy_id <= 5
        )
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    status,
    reported_at,
    total_claim_approved_amount,
    first_claim_reported_at
FROM without_matches
ORDER BY first_claim_reported_at, claimed_amount
