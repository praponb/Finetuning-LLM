WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
joined_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.adjuster_id,
        s.premium,
        a.level AS adjuster_level
    FROM joined_ranges s
    JOIN adjusters a ON a.id = s.adjuster_id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claimed_amount,
        s.adjuster_id,
        s.premium,
        a.adjuster_name,
        p.status AS policy_status
    FROM joined_adjuster s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        premium,
        adjuster_name,
        policy_status,
        premium - claimed_amount AS premium_minus_claimed_amount,
        SUBSTRING(TRIM(policy_status), 1, 3) AS policy_status_prefix,
        CONCAT_WS('-', adjuster_name, policy_status) AS adjuster_name_policy_status_label
    FROM with_parent
    WHERE adjuster_name IS NOT NULL
)
SELECT
    id,
    policy_id,
    adjuster_name,
    policy_status,
    policy_status_prefix,
    adjuster_name_policy_status_label,
    COALESCE(premium, 0.0) AS premium_filled,
    SUBSTRING(TRIM(policy_status), 1, 2) AS policy_status_prefix_2
FROM annotated
ORDER BY policy_id DESC, id
LIMIT 10
