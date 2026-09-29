WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date
    FROM policies
    WHERE YEAR(end_date) = 2025
),
with_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        p.holder_name,
        p.segment AS holder_segment
    FROM scoped s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE UPPER(TRIM(p.holder_name)) LIKE 'N%'
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        c.id AS claim_id,
        c.claim_date,
        c.claim_type,
        c.adjuster_id AS claim_adjuster_id
    FROM with_details_holder s
    LEFT JOIN claims c ON c.policy_id = s.id
)
SELECT
    id,
    holder_id,
    start_date,
    claim_date,
    claim_adjuster_id
FROM detail_rows
ORDER BY claim_date DESC
