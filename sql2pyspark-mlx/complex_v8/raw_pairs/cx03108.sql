WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        LENGTH(TRIM(product)) AS product_len
    FROM policies
    WHERE end_date IS NOT NULL
),
with_activity AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM scoped_policys s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.segment = 'SME'
              AND p.joined_on >= DATE '2024-07-18'
        )
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product_len,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.status AS claim_status
    FROM with_activity s
    LEFT JOIN claims c ON c.policy_id = s.id
),
exploded_detail_2 AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product_len,
        s.claim_id,
        s.claim_reported_at,
        c.id AS claim_id_2,
        c.claim_date,
        c.adjuster_id AS claim_adjuster_id,
        c.claimed_amount AS claim_claimed_amount,
        c.approved_amount AS claim_approved_amount
    FROM exploded_detail s
    JOIN claims c ON c.policy_id = s.id AND c.approved_amount >= 0.0
),
qualified AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM exploded_detail_2 s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.start_date >= DATE '2024-06-19'
        )
)
SELECT
    product_len,
    claim_id,
    claim_id_2,
    ROUND(SUM(claim_claimed_amount), 2) AS total_claim_claimed_amount,
    COUNT(*) AS n_rows
FROM qualified
GROUP BY CUBE(product_len, claim_id, claim_id_2)
ORDER BY product_len NULLS LAST, claim_id NULLS LAST, claim_id_2 NULLS LAST
