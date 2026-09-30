WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        QUARTER(claim_date) AS claim_date_quarter,
        CASE WHEN claim_type IN ('collision', 'medical') THEN 'group_a' WHEN claim_type = 'flood' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        TO_DATE(reported_at) AS reported_at_date
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-31 11:00:00'
      AND policy_id <= 10
),
enriched AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.claim_date_quarter,
        s.claim_type_group,
        s.reported_at_date,
        p.holder_id AS policy_holder_id,
        p.product AS policy_product,
        p.status AS policy_status
    FROM cleaned s
    LEFT JOIN policies p ON p.id = s.policy_id
)
SELECT
    COALESCE(claim_type_group, 'ALL') AS claim_type_group,
    COALESCE(claim_type, 'ALL') AS claim_type,
    adjuster_id,
    GROUPING_ID() AS grouping_level,
    SUM(claimed_amount) AS total_claimed_amount
FROM enriched
GROUP BY CUBE(claim_type_group, claim_type, adjuster_id)
ORDER BY claim_type_group NULLS LAST, claim_type NULLS LAST, adjuster_id NULLS LAST
