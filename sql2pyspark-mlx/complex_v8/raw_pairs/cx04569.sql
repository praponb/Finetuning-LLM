WITH prepared AS (
    -- Scope the claims rows
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
        DATE_ADD(claim_date, 365) AS claim_date_plus_365d,
        CASE WHEN claim_type IN ('fire', 'delay') THEN 'group_a' WHEN claim_type = 'medical' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        LENGTH(TRIM(claim_type)) AS claim_type_len
    FROM claims
    WHERE status = 'IN_REVIEW'
      AND adjuster_id <= 3
),
with_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claim_date_plus_365d,
        s.claim_type_group,
        a.adjuster_name,
        a.team AS adjuster_team,
        p.product AS policy_product,
        p.end_date AS policy_end_date
    FROM prepared s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
)
SELECT id, measure, reading
FROM (
    SELECT id, claimed_amount, approved_amount
    FROM with_details
) src
UNPIVOT (
    reading FOR measure IN (claimed_amount, approved_amount)
) unp
ORDER BY id, measure
