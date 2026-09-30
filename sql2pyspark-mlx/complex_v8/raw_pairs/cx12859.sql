WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        CONCAT_WS(': ', claim_type, status) AS claim_type_status_label,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount,
        CASE WHEN status IN ('APPROVED', 'IN_REVIEW') THEN 'group_a' WHEN status = 'OPEN' THEN 'group_b' ELSE 'other' END AS status_group
    FROM claims
    WHERE approved_amount > 0.0
      AND claim_type IN ('collision', 'fire', 'flood')
),
combined_details_policy AS (
    -- Join policies
    SELECT
        s.*,
        p.holder_id AS policy_holder_id,
        p.end_date AS policy_end_date,
        p.status AS policy_status,
        po.segment AS holder_segment,
        po.risk_score AS holder_risk_score
    FROM cleaned_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM combined_details_policy
    WHERE approved_amount_per_claimed_amount > (SELECT MIN(approved_amount_per_claimed_amount) FROM combined_details_policy)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT status, claim_type, approved_amount_per_claimed_amount
    FROM without_matches
) src
PIVOT (
    MAX(approved_amount_per_claimed_amount)
    FOR claim_type IN ('delay', 'fire', 'theft', 'medical')
)
ORDER BY status
