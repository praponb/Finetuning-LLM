WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_parent AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.approved_amount,
        s.premium,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date
    FROM covered s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.premium < 3532.7
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_parent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.approved_amount < 26237.1
        )
      AND s.status = 'APPROVED'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(status)) AS status_len,
        CASE WHEN status IN ('IN_REVIEW', 'PAID') THEN 'group_a' WHEN status = 'OPEN' THEN 'group_b' ELSE 'other' END AS status_group,
        ABS(claimed_amount - 20436.5) AS claimed_amount_distance
    FROM screened
)
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    policy_start_date,
    status_len,
    status_group,
    claimed_amount_distance,
    claimed_amount + status_len AS claimed_amount_plus_status_len
FROM labeled
ORDER BY claimed_amount_distance NULLS LAST, id
LIMIT 20
