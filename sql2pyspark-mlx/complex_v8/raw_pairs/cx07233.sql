WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE holder_id IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY product, status) AS rows_in_product_status,
        MAX(premium) OVER (PARTITION BY status ORDER BY premium DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_max_2,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY premium DESC, id) AS premium_dense_rank,
        LAST_VALUE(end_date) OVER (PARTITION BY status ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product, status)) AS distinct_holder_id_in_product_status
    FROM prepared
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        end_date,
        rows_in_product_status,
        premium_max_2,
        premium_dense_rank,
        distinct_holder_id_in_product_status,
        DATEDIFF(start_date, last_end_date) AS days_last_end_date_to_start_date
    FROM with_trends
    WHERE premium_dense_rank < 6
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.rows_in_product_status,
        s.premium_max_2,
        s.premium_dense_rank,
        s.distinct_holder_id_in_product_status,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_date,
        c.claim_type
    FROM annotated s
    JOIN claims c ON c.policy_id = s.id AND c.claim_type = 'theft'
),
enriched_claim AS (
    -- Join claims
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.rows_in_product_status,
        s.premium_max_2,
        s.distinct_holder_id_in_product_status,
        s.claim_id,
        s.claim_claimed_amount,
        s.claim_type,
        c.approved_amount AS claim_approved_amount,
        p.product AS policy_product,
        p.premium AS policy_premium
    FROM with_children s
    JOIN claims c ON c.id = s.claim_id
    LEFT JOIN policies p ON p.id = c.policy_id
    WHERE c.approved_amount > 4554.8
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        end_date,
        premium_max_2,
        distinct_holder_id_in_product_status,
        claim_id,
        claim_approved_amount,
        policy_product,
        policy_premium,
        ROUND(claim_approved_amount * 0.9, 2) AS claim_approved_amount_adj,
        CASE WHEN policy_product IN ('HOME', 'TRAVEL') THEN 'group_a' WHEN policy_product = 'LIFE' THEN 'group_b' ELSE 'other' END AS policy_product_group,
        GREATEST(policy_premium, claim_claimed_amount) AS greatest_policy_premium_claim_claimed_amount
    FROM enriched_claim
),
enriched AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium_max_2,
        s.claim_id,
        s.claim_approved_amount,
        s.policy_product,
        s.policy_premium,
        s.claim_approved_amount_adj,
        s.greatest_policy_premium_claim_claimed_amount,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on,
        c.status AS claim_status,
        c.claim_type
    FROM scored s
    JOIN policyholders p ON p.id = s.holder_id
    JOIN claims c ON c.id = s.claim_id
    WHERE YEAR(p.joined_on) = 2024
)
-- Final projection
SELECT
    id,
    holder_id,
    premium_max_2,
    claim_approved_amount,
    policy_premium,
    claim_approved_amount_adj,
    greatest_policy_premium_claim_claimed_amount,
    holder_segment,
    holder_joined_on,
    claim_status,
    GREATEST(policy_premium, premium_max_2) AS greatest_policy_premium_premium_max_2
FROM enriched
WHERE holder_id IS NOT NULL
ORDER BY id NULLS LAST, policy_premium DESC NULLS FIRST
