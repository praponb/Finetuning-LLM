WITH valid_claims AS (
    -- Filter claims
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
    WHERE policy_id >= 10
      AND claim_type = 'flood'
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.reported_at,
        c.id AS claim_id,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_date AS claim_claim_date,
        c.claimed_amount AS claim_claimed_amount
    FROM valid_claims s
    LEFT JOIN claims c ON c.policy_id = s.policy_id AND c.status = 'REJECTED'
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM detail_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND HOUR(c.reported_at) BETWEEN 9 AND 13
        )
),
unique_rows AS (
    -- Keep the earliest row per claim_status, adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        reported_at,
        claim_id,
        claim_status,
        claim_adjuster_id,
        claim_claim_date,
        claim_claimed_amount
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_status, adjuster_id ORDER BY claimed_amount, id, claim_id) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.reported_at,
        s.claim_id,
        s.claim_status,
        s.claim_adjuster_id,
        s.claim_claim_date,
        s.claim_claimed_amount,
        c.id AS claim_id_2,
        c.claim_date,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount
    FROM unique_rows s
    JOIN claims c ON c.policy_id = s.policy_id
),
with_details_claim AS (
    -- Enrich with dimension data
    SELECT s.*, c.policy_id AS claim_policy_id, c.reported_at AS claim_reported_at_2
    FROM with_children s
    JOIN claims c ON c.id = s.claim_id_2 AND YEAR(c.claim_date) = 2024
    WHERE TO_DATE(c.reported_at) < DATE '2025-02-18'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claim_policy_id, policy_id, claim_status, claimed_amount
    FROM with_details_claim
) src
PIVOT (
    SUM(claimed_amount)
    FOR claim_status IN ('APPROVED', 'IN_REVIEW', 'PAID', 'OPEN')
)
