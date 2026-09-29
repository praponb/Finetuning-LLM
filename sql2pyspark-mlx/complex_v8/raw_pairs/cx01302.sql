WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.product,
        c.id AS claim_id,
        c.claim_type,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id,
        c.claimed_amount AS claim_claimed_amount
    FROM matched s
    JOIN claims c ON c.policy_id = s.policy_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(claim_claimed_amount) OVER (PARTITION BY claim_status, claim_type ORDER BY claim_claimed_amount NULLS LAST, id, claim_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS claim_claimed_amount_sum_7,
        NTILE(10) OVER (PARTITION BY claim_type, claim_adjuster_id ORDER BY claim_claimed_amount, id, claim_id) AS claim_claimed_amount_decile,
        COUNT(*) OVER (PARTITION BY adjuster_id) AS rows_in_adjuster_id,
        ROUND(claim_claimed_amount - AVG(claim_claimed_amount) OVER (PARTITION BY adjuster_id), 2) AS claim_claimed_amount_vs_adjuster_id_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_type, claim_adjuster_id ORDER BY claim_claimed_amount), 3) AS claim_claimed_amount_cume_dist
    FROM with_children_claims
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claim_claimed_amount_decile, claim_type, rows_in_adjuster_id
    FROM with_windows
) src
PIVOT (
    MAX(rows_in_adjuster_id)
    FOR claim_type IN ('delay', 'collision')
)
ORDER BY claim_claimed_amount_decile
