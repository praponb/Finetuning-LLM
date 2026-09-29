WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at
    FROM claims
    WHERE MONTH(reported_at) IN (1, 2)
      AND status = 'OPEN'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        reported_at,
        LEAD(reported_at) OVER w_claim_date_year_policy_id_by AS next_reported_at,
        NTH_VALUE(approved_amount, 2) OVER (ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        LEAD(claim_date) OVER (ORDER BY approved_amount DESC, id) AS next_claim_date,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY adjuster_id, policy_id), 2) AS approved_amount_vs_adjuster_id_policy_id_avg,
        LAG(claim_date) OVER (ORDER BY approved_amount DESC, id) AS prev_claim_date,
        LAST_VALUE(approved_amount) OVER (PARTITION BY claim_type ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount,
        NTILE(4) OVER w_claim_date_year_policy_id_by AS claim_date_quartile,
        SUM(approved_amount) OVER (PARTITION BY YEAR(claim_date), policy_id ORDER BY claim_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_sum_3
    FROM recent_claims
    WINDOW w_claim_date_year_policy_id_by AS (PARTITION BY YEAR(claim_date), policy_id ORDER BY claim_date DESC, id)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    reported_at,
    next_reported_at,
    second_approved_amount,
    next_claim_date,
    approved_amount_vs_adjuster_id_policy_id_avg,
    prev_claim_date,
    last_approved_amount,
    claim_date_quartile,
    approved_amount_sum_3,
    COALESCE(last_approved_amount, 0.0) AS last_approved_amount_filled
FROM sequenced
ORDER BY claim_date_quartile NULLS LAST, claim_type, id
LIMIT 10
