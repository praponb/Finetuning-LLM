WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref,
        CASE WHEN premium >= 2945.9 THEN 'A' WHEN premium >= 1541.8 THEN 'B' ELSE 'C' END AS premium_tier
    FROM policies
    WHERE holder_id IS NOT NULL
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.premium_tier,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.adjuster_id AS claim_adjuster_id
    FROM base_policys s
    LEFT JOIN claims c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    start_date,
    end_date,
    premium_tier,
    claim_claimed_amount,
    claim_adjuster_id,
    DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label
FROM exploded_detail_claims
WHERE premium_tier IN ('C', 'A')
ORDER BY claim_adjuster_id, premium_tier DESC
