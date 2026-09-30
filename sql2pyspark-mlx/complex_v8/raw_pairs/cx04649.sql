WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        CASE WHEN status IN ('ACTIVE', 'EXPIRED') THEN 'group_a' WHEN status = 'CANCELLED' THEN 'group_b' ELSE 'other' END AS status_group,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE product = 'HOME'
      AND start_date < DATE '2025-03-06'
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM cleaned_policys s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.adjuster_id IS NOT NULL
              AND c.status IS NOT NULL
        )
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status_group,
        s.premium_filled,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on,
        p.region AS holder_region
    FROM qualified s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.holder_risk_score,
        s.holder_joined_on,
        s.holder_region,
        c.id AS claim_id,
        c.claim_type,
        c.claimed_amount AS claim_claimed_amount
    FROM with_parent s
    JOIN claims c ON c.policy_id = s.id AND c.claimed_amount < 41878.5
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT product, holder_region, claim_claimed_amount
    FROM detail_rows
) src
PIVOT (
    SUM(claim_claimed_amount) AS total, COUNT(claim_claimed_amount) AS n
    FOR holder_region IN ('North', 'Central')
)
ORDER BY product
