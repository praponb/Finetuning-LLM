WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
latest AS (
    -- Keep the latest row per claim_type, policy_id
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        claim_type,
        adjuster_id,
        product,
        premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type, policy_id ORDER BY premium DESC, id DESC) AS row_num
        FROM joined_ranges
    ) ranked
    WHERE row_num = 1
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        ROUND(premium * 1.1, 2) AS premium_adj
    FROM latest
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.adjuster_id,
        s.premium,
        s.claimed_amount_filled,
        a.team AS adjuster_team,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM labeled s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team IN ('Health', 'Motor', 'Property')
)
-- Final projection
SELECT
    id,
    claim_date,
    claimed_amount,
    adjuster_id,
    claimed_amount_filled,
    adjuster_team,
    adjuster_name,
    adjuster_level,
    DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date
FROM with_parent
WHERE adjuster_level >= 3
ORDER BY claimed_amount, claimed_amount_filled DESC
