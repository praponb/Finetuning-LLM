WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        DATEDIFF(DATE '2025-12-31', claim_date) AS days_since_claim_date,
        CASE WHEN approved_amount >= 23487.5 THEN 'HIGH' WHEN approved_amount >= 799.5 THEN 'MEDIUM' ELSE 'LOW' END AS approved_amount_tier
    FROM claims
    WHERE claim_type IS NOT NULL
),
combined_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.status,
        p.end_date AS policy_end_date,
        p.holder_id AS policy_holder_id,
        po.region AS holder_region,
        po.holder_name
    FROM filtered_claims s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(holder_name, '#', CAST(id AS STRING)) AS holder_name_ref,
        CASE WHEN claim_type IN ('delay', 'fire') THEN 'group_a' WHEN claim_type = 'flood' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        CASE WHEN status IN ('OPEN', 'REJECTED') THEN 'group_a' WHEN status = 'PAID' THEN 'group_b' ELSE 'other' END AS status_group
    FROM combined_details_policy
    WHERE holder_name IS NOT NULL
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    status,
    policy_end_date,
    policy_holder_id,
    holder_region,
    holder_name,
    holder_name_ref,
    claim_type_group,
    status_group,
    DATE_FORMAT(policy_end_date, 'yyyy-MM-dd') AS policy_end_date_label
FROM tagged
ORDER BY id DESC, policy_end_date DESC NULLS FIRST
