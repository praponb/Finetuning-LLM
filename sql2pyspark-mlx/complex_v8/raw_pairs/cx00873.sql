WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.approved_amount,
        s.adjuster_id,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at,
        c.claim_type,
        c.claim_date AS claim_claim_date
    FROM covered s
    JOIN claims c ON c.policy_id = s.policy_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(claim_reported_at, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_claim_date) ORDER BY claim_reported_at DESC, id, claim_id) AS next3_claim_reported_at,
        SIZE(COLLECT_SET(adjuster_id) OVER w_claim_id_policy_id) AS distinct_adjuster_id_in_claim_id_policy_id,
        RANK() OVER w_claim_claim_date_month_by_cl_2 AS claim_reported_at_rank,
        SIZE(COLLECT_SET(claim_type) OVER w_claim_id_policy_id) AS distinct_claim_type_in_claim_id_policy_id,
        MIN(claim_approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_claim_date) ORDER BY claim_reported_at DESC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claim_approved_amount_so_far
    FROM detail_rows_claims
    WINDOW w_claim_id_policy_id AS (PARTITION BY claim_id, policy_id), w_claim_claim_date_month_by_cl_2 AS (PARTITION BY DATE_TRUNC('MONTH', claim_claim_date) ORDER BY claim_reported_at DESC)
)
SELECT
    id,
    adjuster_id,
    claim_id,
    claim_approved_amount,
    claim_reported_at,
    claim_type,
    claim_claim_date,
    next3_claim_reported_at,
    distinct_adjuster_id_in_claim_id_policy_id,
    claim_reported_at_rank,
    distinct_claim_type_in_claim_id_policy_id,
    min_claim_approved_amount_so_far,
    DATE_TRUNC('YEAR', next3_claim_reported_at) AS next3_claim_reported_at_year
FROM with_windows
WHERE min_claim_approved_amount_so_far >= 175000.0
ORDER BY adjuster_id NULLS LAST, claim_type
