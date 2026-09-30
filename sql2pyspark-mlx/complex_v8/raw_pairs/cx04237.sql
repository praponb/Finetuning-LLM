WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM policies
    WHERE YEAR(start_date) = 2025
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.status_or_unknown,
        s.product_slug,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM scoped_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.product_slug,
        s.holder_segment,
        s.holder_joined_on,
        s.holder_risk_score,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.approved_amount AS claim_approved_amount,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_type
    FROM with_parent s
    JOIN claims c ON c.policy_id = s.id AND c.claimed_amount < 32796.4
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.product_slug,
        s.holder_joined_on,
        s.holder_risk_score,
        s.claim_id,
        s.claim_claimed_amount,
        s.claim_approved_amount,
        s.claim_adjuster_id,
        s.claim_type,
        p.risk_score AS holder_risk_score_2
    FROM line_level s
    JOIN policyholders p ON p.id = s.holder_id
),
product_stats AS (
    -- Avg claim_approved_amount per product
    SELECT product, AVG(claim_approved_amount) AS avg_claim_approved_amount
    FROM joined_holder
    GROUP BY product
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claim_approved_amount,
        ROUND(s.claim_approved_amount - g.avg_claim_approved_amount, 2) AS claim_approved_amount_minus_avg
    FROM joined_holder s
    JOIN product_stats g ON g.product = s.product
    WHERE s.claim_approved_amount > g.avg_claim_approved_amount
)
SELECT
    id,
    holder_id,
    product,
    end_date,
    product_slug,
    holder_risk_score,
    claim_claimed_amount,
    claim_approved_amount,
    claim_adjuster_id,
    claim_type,
    holder_risk_score_2,
    avg_claim_approved_amount,
    LEAST(holder_risk_score, claim_approved_amount) AS least_holder_risk_score_claim_approved_amount
FROM outliers
WHERE claim_claimed_amount < 29336.0
ORDER BY id NULLS LAST
