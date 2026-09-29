WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        ABS(premium - 2844.1) AS premium_distance,
        DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date,
        ROUND(premium * 1.1, 2) AS premium_adj
    FROM policies
    WHERE holder_id <= 8
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.premium_distance,
        s.days_since_end_date,
        s.premium_adj,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_type
    FROM recent s
    LEFT JOIN claims c ON c.policy_id = s.id
),
with_details AS (
    -- Attach holder attributes
    SELECT s.*, p.segment AS holder_segment, p.joined_on AS holder_joined_on
    FROM line_level s
    JOIN policyholders p ON p.id = s.holder_id AND p.region IS NOT NULL
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.premium_adj,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_type,
        s.holder_segment,
        s.holder_joined_on,
        c.id AS claim_id_2,
        c.claim_date,
        c.adjuster_id AS claim_adjuster_id_2,
        c.approved_amount AS claim_approved_amount
    FROM with_details s
    JOIN claims c ON c.policy_id = s.id
),
qualified AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM exploded_detail_claims s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND YEAR(p.end_date) = 2026
        )
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(claim_approved_amount / NULLIF(premium_adj, 0), 3) AS claim_approved_amount_per_premium_adj
    FROM qualified
)
SELECT
    id,
    holder_id,
    end_date,
    claim_id,
    claim_adjuster_id,
    claim_type,
    holder_segment,
    holder_joined_on,
    claim_id_2,
    claim_date,
    claim_adjuster_id_2,
    claim_approved_amount,
    claim_approved_amount_per_premium_adj,
    GREATEST(premium_adj, claim_approved_amount) AS greatest_premium_adj_claim_approved_amount,
    ROUND(claim_approved_amount / NULLIF(premium_adj, 0), 3) AS claim_approved_amount_per_premium_adj_2
FROM derived
WHERE claim_adjuster_id IS NOT NULL
ORDER BY claim_approved_amount_per_premium_adj DESC
