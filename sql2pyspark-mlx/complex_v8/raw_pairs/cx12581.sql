WITH recent AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_FORMAT(end_date, 'MMM yyyy') AS end_date_label,
        CASE WHEN status IN ('EXPIRED', 'ACTIVE') THEN 'group_a' WHEN status = 'CANCELLED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
    WHERE product NOT IN ('AUTO', 'LIFE')
),
filtered AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.end_date >= DATE '2025-11-20'
              AND p.status = 'ACTIVE'
        )
      AND YEAR(s.end_date) = 2025
),
with_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.end_date_label,
        s.status_group,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM filtered s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status_group,
        s.holder_joined_on,
        s.holder_risk_score,
        c.id AS claim_id,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id
    FROM with_details_holder s
    JOIN claims c ON c.policy_id = s.id AND c.status <> 'APPROVED'
),
combined_details_claim AS (
    -- Attach claim attributes
    SELECT
        s.*,
        c.adjuster_id AS claim_adjuster_id_2,
        c.approved_amount AS claim_approved_amount,
        p.product AS policy_product,
        p.start_date AS policy_start_date
    FROM exploded_detail s
    LEFT JOIN claims c ON c.id = s.claim_id
    LEFT JOIN policies p ON p.id = c.policy_id
)
SELECT
    id,
    holder_id,
    product,
    status_group,
    holder_joined_on,
    holder_risk_score,
    claim_status,
    claim_adjuster_id,
    claim_adjuster_id_2,
    claim_approved_amount,
    policy_product,
    policy_start_date
FROM combined_details_claim
WHERE claim_id IS NOT NULL
ORDER BY policy_start_date, claim_adjuster_id DESC
