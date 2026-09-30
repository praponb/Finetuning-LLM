WITH prepared_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE policy_id <= 17
),
combined_details AS (
    -- Attach policy attributes
    SELECT
        s.*,
        p.start_date AS policy_start_date,
        p.premium AS policy_premium,
        p.product AS policy_product
    FROM prepared_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.policy_start_date,
        s.policy_product,
        c.id AS claim_id,
        c.claim_type AS claim_claim_type,
        c.approved_amount AS claim_approved_amount
    FROM combined_details s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
)
SELECT
    claim_claim_type,
    claim_id,
    policy_product,
    COUNT(*) AS cnt
FROM exploded_detail
GROUP BY CUBE(claim_claim_type, claim_id, policy_product)
ORDER BY claim_claim_type NULLS LAST, claim_id NULLS LAST, policy_product NULLS LAST
