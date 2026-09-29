WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label
    FROM policies
    WHERE end_date >= DATE '2026-01-20'
      AND start_date BETWEEN DATE '2024-06-19' AND DATE '2025-05-02'
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.status,
        s.start_date_label,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on
    FROM cleaned s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.start_date_label,
        s.holder_segment,
        s.holder_joined_on,
        p.segment AS holder_segment_2,
        p.joined_on AS holder_joined_on_2
    FROM joined_holder s
    JOIN policyholders p ON p.id = s.holder_id AND UPPER(TRIM(p.holder_name)) LIKE 'D%'
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status,
        s.holder_segment,
        s.holder_joined_on,
        s.holder_segment_2,
        s.holder_joined_on_2,
        c.id AS claim_id,
        c.claim_date,
        c.claimed_amount AS claim_claimed_amount,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount
    FROM combined_details_holder s
    LEFT JOIN claims c ON c.policy_id = s.id AND c.claimed_amount < 29359.4
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claim_id, status, claim_approved_amount
    FROM exploded_detail_claims
) src
PIVOT (
    MAX(claim_approved_amount)
    FOR status IN ('EXPIRED', 'CANCELLED', 'LAPSED')
)
ORDER BY claim_id
