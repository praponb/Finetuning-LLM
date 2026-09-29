WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date
    FROM policies
    WHERE status = 'CANCELLED'
      AND premium <= 3544.1
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount,
        c.status AS claim_status
    FROM prepared s
    JOIN claims c ON c.policy_id = s.id
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.claim_id,
        s.claim_approved_amount,
        s.claim_status,
        c.id AS claim_id_2,
        c.adjuster_id AS claim_adjuster_id,
        c.approved_amount AS claim_approved_amount_2
    FROM with_children s
    JOIN claims c ON c.policy_id = s.id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(' / ', product, claim_status) AS product_claim_status_label
    FROM with_children_claims
    WHERE claim_approved_amount_2 > 0.0
)
SELECT id, claim_id, field, reading
FROM (
    SELECT id, claim_id, claim_approved_amount, claim_approved_amount_2
    FROM enriched
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (claim_approved_amount, claim_approved_amount_2)
) unp
ORDER BY id, claim_id, field
