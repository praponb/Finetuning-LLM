WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label,
        CONCAT(claim_type, '#', CAST(policy_id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE claimed_amount < 42447.2
),
with_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.status_or_unknown,
        s.claim_date_label,
        s.claim_type_ref,
        p.status AS policy_status,
        p.product AS policy_product,
        po.joined_on AS holder_joined_on
    FROM recent s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(policy_product, 'ALL') AS policy_product,
    adjuster_id,
    COALESCE(status_or_unknown, 'ALL') AS status_or_unknown,
    GROUPING(policy_product) AS is_total_policy_product,
    ROUND(SUM(claimed_amount) / NULLIF(SUM(approved_amount), 0), 3) AS claimed_amount_to_approved_amount_ratio,
    SUM(CASE WHEN claim_type = 'fire' THEN 1 ELSE 0 END) AS n_claim_type_fire,
    COUNT(DISTINCT status) AS distinct_status
FROM with_details_policy
GROUP BY GROUPING SETS ((policy_product, adjuster_id, status_or_unknown), (policy_product), (adjuster_id))
