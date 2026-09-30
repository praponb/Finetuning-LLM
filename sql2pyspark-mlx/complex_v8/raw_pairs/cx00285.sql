WITH cleaned_claims AS (
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
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 6 AND 11
),
with_details AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claimed_amount_bucket,
        p.end_date AS policy_end_date,
        p.holder_id AS policy_holder_id,
        p.product AS policy_product,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM cleaned_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
    JOIN adjusters a ON a.id = s.adjuster_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_adjuster_level_claim_type AS rows_in_adjuster_level_claim_type,
        SUM(claimed_amount) OVER (PARTITION BY policy_product, policy_id) AS policy_product_policy_id_sum_claimed_amount,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_adjuster_level_claim_type, 0), 4) AS approved_amount_share_of_adjuster_level_claim_ty,
        LAG(claimed_amount, 3, 0) OVER (PARTITION BY policy_product, DATE_TRUNC('MONTH', claim_date) ORDER BY claim_date DESC, id) AS prev3_claimed_amount,
        MAX(claimed_amount_bucket) OVER (ORDER BY adjuster_level DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS claimed_amount_bucket_max_3_prior,
        NTH_VALUE(policy_end_date, 2) OVER (ORDER BY claimed_amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_policy_end_date,
        ROW_NUMBER() OVER w_all_by_claimed_amount_bucket_2 AS rn,
        RANK() OVER (PARTITION BY status ORDER BY adjuster_level DESC) AS adjuster_level_rank
    FROM with_details
    WINDOW w_adjuster_level_claim_type AS (PARTITION BY adjuster_level, claim_type), w_all_by_claimed_amount_bucket_2 AS (ORDER BY claimed_amount_bucket, id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        QUARTER(second_policy_end_date) AS second_policy_end_date_quarter,
        CONCAT(claim_type, '#', CAST(claimed_amount_bucket AS STRING)) AS claim_type_ref,
        CONCAT_WS('|', claim_type, policy_product) AS claim_type_policy_product_label
    FROM sequenced
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    approved_amount,
    claimed_amount_bucket,
    policy_end_date,
    policy_holder_id,
    policy_product,
    adjuster_level,
    adjuster_team,
    rows_in_adjuster_level_claim_type,
    policy_product_policy_id_sum_claimed_amount,
    approved_amount_share_of_adjuster_level_claim_ty,
    prev3_claimed_amount,
    second_policy_end_date,
    rn,
    adjuster_level_rank,
    second_policy_end_date_quarter,
    claim_type_ref,
    claim_type_policy_product_label,
    ADD_MONTHS(policy_end_date, 12) AS policy_end_date_plus_12m
FROM labeled
ORDER BY approved_amount DESC, approved_amount_share_of_adjuster_level_claim_ty DESC
