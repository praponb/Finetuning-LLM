WITH base_claims AS (
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
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount,
        CASE WHEN status IN ('OPEN', 'PAID') THEN 'group_a' WHEN status = 'IN_REVIEW' THEN 'group_b' ELSE 'other' END AS status_group
    FROM claims
    WHERE policy_id <= 26
      AND status = 'IN_REVIEW'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        greatest_claimed_amount_approved_amount,
        ROUND(CUME_DIST() OVER (ORDER BY greatest_claimed_amount_approved_amount DESC, id), 3) AS greatest_claimed_amount_approved_amount_cume_dis,
        LAG(claimed_amount, 3) OVER w_status_group_policy_id_by_ap AS prev3_claimed_amount,
        COUNT(*) OVER (ORDER BY greatest_claimed_amount_approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY status_group, claim_type), 0), 4) AS approved_amount_share_of_status_group_claim_type,
        ROW_NUMBER() OVER (PARTITION BY status_group, policy_id ORDER BY approved_amount, id) AS approved_amount_seq,
        MAX(approved_amount) OVER (PARTITION BY status_group, claim_type) AS status_group_claim_type_max_approved_amount,
        COUNT(*) OVER (PARTITION BY status_group, claim_type) AS rows_in_status_group_claim_type,
        LAG(claimed_amount) OVER w_status_group_policy_id_by_ap AS prev_claimed_amount,
        MAX(claimed_amount) OVER (PARTITION BY status_group, claim_type) AS status_group_claim_type_max_claimed_amount,
        ROW_NUMBER() OVER w_status_group_policy_id_by_gr AS greatest_claimed_amount_approved_amount_seq,
        NTILE(4) OVER (PARTITION BY status_group, policy_id ORDER BY greatest_claimed_amount_approved_amount NULLS LAST, id) AS greatest_claimed_amount_approved_amount_quartile,
        DENSE_RANK() OVER (ORDER BY greatest_claimed_amount_approved_amount DESC, id) AS greatest_claimed_amount_approved_amount_dense_ra,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY status_group, policy_id ORDER BY greatest_claimed_amount_approved_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        SUM(approved_amount) OVER (PARTITION BY status_group, claim_type) AS status_group_claim_type_sum_approved_amount
    FROM base_claims
    WINDOW w_status_group_policy_id_by_ap AS (PARTITION BY status_group, policy_id ORDER BY approved_amount, id), w_all_by_greatest_claimed_amou AS (ORDER BY greatest_claimed_amount_approved_amount DESC, id), w_status_group_policy_id_by_gr AS (PARTITION BY status_group, policy_id ORDER BY greatest_claimed_amount_approved_amount NULLS LAST, id)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(claim_date, 2) OVER w_all_by_claimed_amount AS next2_claim_date,
        FIRST_VALUE(second_approved_amount) IGNORE NULLS OVER (PARTITION BY policy_id, greatest_claimed_amount_approved_amount_quartile ORDER BY approved_amount_seq DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_second_approved_amount,
        ROW_NUMBER() OVER (PARTITION BY approved_amount_seq ORDER BY greatest_claimed_amount_approved_amount_quartile ASC, id) AS rn,
        LAG(prev3_claimed_amount) OVER w_all_by_claimed_amount AS prev_prev3_claimed_amount
    FROM with_windows
    WINDOW w_all_by_claimed_amount AS (ORDER BY claimed_amount ASC, id)
)
SELECT
    id,
    policy_id,
    claim_date,
    approved_amount,
    status,
    greatest_claimed_amount_approved_amount,
    greatest_claimed_amount_approved_amount_cume_dis,
    prev3_claimed_amount,
    running_count,
    approved_amount_share_of_status_group_claim_type,
    approved_amount_seq,
    status_group_claim_type_max_approved_amount,
    prev_claimed_amount,
    status_group_claim_type_max_claimed_amount,
    greatest_claimed_amount_approved_amount_seq,
    greatest_claimed_amount_approved_amount_quartile,
    second_approved_amount,
    status_group_claim_type_sum_approved_amount,
    next2_claim_date,
    first_known_second_approved_amount,
    UPPER(TRIM(status)) AS status_clean,
    ADD_MONTHS(next2_claim_date, 6) AS next2_claim_date_plus_6m
FROM windowed
