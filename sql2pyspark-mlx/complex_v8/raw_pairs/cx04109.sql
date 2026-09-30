WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE end_date BETWEEN DATE '2025-11-20' AND DATE '2026-08-31'
      AND start_date IS NOT NULL
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        c.id AS claim_id,
        c.status AS claim_status,
        c.approved_amount AS claim_approved_amount
    FROM base s
    JOIN claims c ON c.policy_id = s.id AND c.status IN ('IN_REVIEW', 'PAID', 'OPEN')
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.claim_id,
        s.claim_approved_amount,
        c.id AS claim_id_2,
        c.claimed_amount AS claim_claimed_amount,
        c.adjuster_id AS claim_adjuster_id,
        c.approved_amount AS claim_approved_amount_2,
        c.claim_type
    FROM exploded_detail s
    JOIN claims c ON c.policy_id = s.id AND c.claimed_amount < 31715.7
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, MIN(approved_amount) AS min_claim_approved_amount
    FROM claims
    GROUP BY adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.min_claim_approved_amount
    FROM with_children_claims s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.claim_adjuster_id
)
SELECT
    holder_id,
    claim_id,
    product,
    GROUPING(holder_id) AS is_total_holder_id,
    SUM(claim_claimed_amount) AS total_claim_claimed_amount,
    ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
    ROUND(SUM(claim_approved_amount), 2) AS total_claim_approved_amount
FROM with_counts
GROUP BY GROUPING SETS ((holder_id, claim_id, product), (holder_id), ())
ORDER BY holder_id NULLS LAST, claim_id NULLS LAST, product NULLS LAST
