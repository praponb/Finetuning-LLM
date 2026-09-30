WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        ROUND(premium * 0.25, 2) AS premium_adj
    FROM policies
    WHERE holder_id IS NOT NULL
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.premium_bucket,
        c.id AS claim_id,
        c.claim_type,
        c.reported_at AS claim_reported_at,
        c.claimed_amount AS claim_claimed_amount
    FROM prepared s
    JOIN claims c ON c.policy_id = s.id AND c.claimed_amount <= 29359.4
),
claim_type_stats AS (
    -- Max claim_claimed_amount per claim_type
    SELECT claim_type, MAX(claim_claimed_amount) AS max_claim_claimed_amount
    FROM line_level_claims
    GROUP BY claim_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_claim_claimed_amount,
        ROUND(s.claim_claimed_amount - g.max_claim_claimed_amount, 2) AS claim_claimed_amount_minus_max
    FROM line_level_claims s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.claim_claimed_amount = g.max_claim_claimed_amount
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        end_date,
        premium_bucket,
        claim_id,
        claim_reported_at,
        max_claim_claimed_amount,
        SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix,
        CASE WHEN claim_claimed_amount >= 30115.9 THEN 'A' WHEN claim_claimed_amount >= 19609.8 THEN 'B' ELSE 'C' END AS claim_claimed_amount_tier
    FROM vs_group
    WHERE claim_reported_at >= TIMESTAMP '2025-01-19 13:00:00'
)
-- Subtotals with CUBE
SELECT
    COALESCE(claim_claimed_amount_tier, 'ALL') AS claim_claimed_amount_tier,
    holder_id,
    ROUND(AVG(max_claim_claimed_amount), 2) AS avg_max_claim_claimed_amount,
    MIN(max_claim_claimed_amount) AS min_max_claim_claimed_amount
FROM scored
GROUP BY CUBE(claim_claimed_amount_tier, holder_id)
ORDER BY claim_claimed_amount_tier NULLS LAST, holder_id NULLS LAST
