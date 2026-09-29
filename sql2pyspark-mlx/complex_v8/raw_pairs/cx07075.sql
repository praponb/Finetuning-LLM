WITH matched AS (
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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(claimed_amount) OVER (PARTITION BY status, adjuster_id) AS status_adjuster_id_max_claimed_amount,
        DENSE_RANK() OVER (PARTITION BY adjuster_id, status ORDER BY premium, id) AS premium_dense_rank,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY adjuster_id, status ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY adjuster_id, status ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount
    FROM matched
),
with_parent AS (
    -- Join adjusters
    SELECT
        s.*,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM with_windows s
    JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    claim_date,
    status,
    product,
    premium,
    status_adjuster_id_max_claimed_amount,
    premium_dense_rank,
    first_known_claim_date,
    last_known_claim_date,
    last_known_claimed_amount,
    adjuster_team
FROM with_parent
ORDER BY adjuster_team, status_adjuster_id_max_claimed_amount NULLS LAST
