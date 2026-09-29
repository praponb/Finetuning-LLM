WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        INITCAP(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 7 AND 9
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.claim_type_clean,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_date,
        c.status AS claim_status,
        c.claim_type AS claim_claim_type
    FROM base s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
),
with_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.claim_type_clean,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_date,
        s.claim_status,
        s.claim_claim_type,
        p.premium AS policy_premium,
        p.holder_id AS policy_holder_id,
        p.product AS policy_product
    FROM with_children_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_details_policy s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND HOUR(c.reported_at) BETWEEN 9 AND 13
        )
)
-- Subtotals with CUBE
SELECT
    COALESCE(policy_product, 'ALL') AS policy_product,
    COALESCE(claim_claim_type, 'ALL') AS claim_claim_type,
    policy_id,
    GROUPING_ID() AS gid,
    MIN(claim_date) AS first_claim_date,
    ROUND(STDDEV(claimed_amount), 2) AS stddev_claimed_amount
FROM filtered
GROUP BY CUBE(policy_product, claim_claim_type, policy_id)
ORDER BY policy_product NULLS LAST, claim_claim_type NULLS LAST, policy_id NULLS LAST
