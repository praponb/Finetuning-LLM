WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
),
with_parent_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.greatest_approved_amount_claimed_amount,
        p.holder_id AS policy_holder_id
    FROM base_claims s
    JOIN policies p ON p.id = s.policy_id AND p.status IN ('EXPIRED', 'ACTIVE')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        claim_type,
        approved_amount,
        policy_holder_id,
        LEAD(claim_date, 2) OVER (PARTITION BY claim_type, policy_holder_id ORDER BY claim_date, id) AS next2_claim_date,
        ROUND(PERCENT_RANK() OVER w_all_by_greatest_approved_amo, 3) AS greatest_approved_amount_claimed_amount_percent_,
        ROUND(approved_amount - AVG(approved_amount) OVER w_adjuster_id_claim_type, 2) AS approved_amount_vs_adjuster_id_claim_type_avg,
        DENSE_RANK() OVER (PARTITION BY claim_type, policy_holder_id ORDER BY claim_date) AS claim_date_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type, policy_holder_id ORDER BY claim_date), 3) AS claim_date_percent_rank,
        NTH_VALUE(claim_date, 2) OVER (ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        ROW_NUMBER() OVER (PARTITION BY claim_type, policy_holder_id ORDER BY claim_date, id) AS rn,
        LAG(approved_amount) OVER (PARTITION BY claim_type, policy_holder_id ORDER BY claim_date, id) AS prev_approved_amount,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (ORDER BY greatest_approved_amount_claimed_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        MIN(approved_amount) OVER (PARTITION BY adjuster_id, claim_type) AS adjuster_id_claim_type_min_approved_amount,
        approved_amount - LAG(approved_amount) OVER (ORDER BY greatest_approved_amount_claimed_amount NULLS LAST, id) AS approved_amount_change,
        NTILE(5) OVER (PARTITION BY claim_type, policy_holder_id ORDER BY claim_date, id) AS claim_date_quintile
    FROM with_parent_policy
    WINDOW w_all_by_greatest_approved_amo AS (ORDER BY greatest_approved_amount_claimed_amount NULLS LAST, id), w_adjuster_id_claim_type AS (PARTITION BY adjuster_id, claim_type)
),
leaders AS (
    -- Top 5 per group
    SELECT
        id,
        adjuster_id,
        claim_type,
        approved_amount,
        policy_holder_id,
        next2_claim_date,
        greatest_approved_amount_claimed_amount_percent_,
        approved_amount_vs_adjuster_id_claim_type_avg,
        claim_date_dense_rank,
        claim_date_percent_rank,
        second_claim_date,
        prev_approved_amount,
        last_known_claim_date,
        adjuster_id_claim_type_min_approved_amount,
        approved_amount_change,
        claim_date_quintile
    FROM with_windows
    WHERE rn <= 5
)
SELECT
    id,
    claim_type,
    approved_amount,
    policy_holder_id,
    next2_claim_date,
    greatest_approved_amount_claimed_amount_percent_,
    approved_amount_vs_adjuster_id_claim_type_avg,
    claim_date_dense_rank,
    claim_date_percent_rank,
    prev_approved_amount,
    last_known_claim_date,
    adjuster_id_claim_type_min_approved_amount,
    approved_amount_change,
    DATE_ADD(last_known_claim_date, 365) AS last_known_claim_date_plus_365d,
    LENGTH(TRIM(claim_type)) AS claim_type_len
FROM leaders
WHERE approved_amount_vs_adjuster_id_claim_type_avg < 15000.0
