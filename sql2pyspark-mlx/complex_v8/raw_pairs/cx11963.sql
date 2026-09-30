WITH scoped AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        CASE WHEN status IN ('IN_REVIEW', 'OPEN') THEN 'group_a' WHEN status = 'REJECTED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM claims
    WHERE claim_type = 'theft'
      AND approved_amount > 0.0
),
with_parent AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.status,
        s.status_group,
        p.product AS policy_product,
        p.start_date AS policy_start_date
    FROM scoped s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.product = 'HOME'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_claim_date_month_policy_prod AS rows_in_claim_date_month_policy_product,
        LAG(policy_start_date) OVER (PARTITION BY adjuster_id ORDER BY policy_start_date DESC, id) AS prev_policy_start_date,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY MONTH(claim_date), policy_product), 0), 4) AS approved_amount_share_of_claim_date_month_policy,
        DENSE_RANK() OVER (ORDER BY policy_start_date, id) AS policy_start_date_dense_rank,
        ROUND(AVG(approved_amount) OVER (ORDER BY policy_start_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_3_prior,
        ROUND(AVG(approved_amount) OVER (PARTITION BY adjuster_id ORDER BY policy_start_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_3,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_id)) AS distinct_adjuster_id_in_policy_id,
        FIRST_VALUE(policy_start_date) OVER (ORDER BY policy_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_policy_start_date,
        SUM(approved_amount) OVER (PARTITION BY policy_id) AS policy_id_sum_approved_amount,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY adjuster_id ORDER BY policy_start_date DESC, id)) AS days_since_prev_claim_date,
        SIZE(COLLECT_SET(status_group) OVER (PARTITION BY policy_id)) AS distinct_status_group_in_policy_id,
        ROW_NUMBER() OVER w_all_by_policy_start_date AS seq_no,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_all_by_approved_amount) AS days_since_prev_claim_date_2,
        MAX(approved_amount) OVER (ORDER BY policy_start_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_max_3,
        MAX(approved_amount) OVER w_policy_id AS policy_id_max_approved_amount
    FROM with_parent
    WINDOW w_claim_date_month_policy_prod AS (PARTITION BY MONTH(claim_date), policy_product), w_all_by_policy_start_date AS (ORDER BY policy_start_date, id), w_all_by_approved_amount AS (ORDER BY approved_amount DESC, id), w_policy_id AS (PARTITION BY policy_id)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(approved_amount, seq_no) AS greatest_approved_amount_seq_no,
        CASE WHEN approved_amount_max_3 >= 127500.0 THEN 'large' WHEN approved_amount_max_3 >= 30000.0 THEN 'medium' ELSE 'small' END AS approved_amount_max_3_tier,
        LEAST(seq_no, approved_amount) AS least_seq_no_approved_amount
    FROM with_windows
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        approved_amount,
        status_group,
        policy_product,
        rows_in_claim_date_month_policy_product,
        prev_policy_start_date,
        approved_amount_share_of_claim_date_month_policy,
        approved_amount_avg_3,
        first_policy_start_date,
        policy_id_sum_approved_amount,
        seq_no,
        approved_amount_max_3,
        policy_id_max_approved_amount,
        greatest_approved_amount_seq_no,
        approved_amount_max_3_tier,
        least_seq_no_approved_amount,
        SIZE(COLLECT_SET(policy_product) OVER w_policy_id_approved_amount_ma) AS distinct_policy_product_in_policy_id_approved_am,
        ROUND(AVG(approved_amount_avg_3) OVER (PARTITION BY distinct_status_group_in_policy_id, status_group ORDER BY greatest_approved_amount_seq_no ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_3_avg_6_prior,
        RANK() OVER (PARTITION BY MONTH(prev_policy_start_date), status ORDER BY policy_start_date_dense_rank, id) AS policy_start_date_dense_rank_rank,
        NTH_VALUE(days_since_prev_claim_date, 2) OVER (PARTITION BY MONTH(prev_policy_start_date), status ORDER BY policy_start_date_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_prev_claim_date,
        FIRST_VALUE(least_seq_no_approved_amount) OVER (PARTITION BY distinct_status_group_in_policy_id, status_group ORDER BY greatest_approved_amount_seq_no ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_least_seq_no_approved_amount,
        COUNT(*) OVER w_first_policy_start_date_mont AS rows_in_first_policy_start_date_month_seq_no
    FROM labeled
    WINDOW w_policy_id_approved_amount_ma AS (PARTITION BY policy_id, approved_amount_max_3_tier), w_first_policy_start_date_mont AS (PARTITION BY DATE_TRUNC('MONTH', first_policy_start_date), seq_no)
)
-- Final projection
SELECT
    id,
    policy_id,
    approved_amount,
    status_group,
    rows_in_claim_date_month_policy_product,
    prev_policy_start_date,
    approved_amount_share_of_claim_date_month_policy,
    approved_amount_avg_3,
    first_policy_start_date,
    policy_id_sum_approved_amount,
    approved_amount_max_3,
    policy_id_max_approved_amount,
    greatest_approved_amount_seq_no,
    approved_amount_max_3_tier,
    approved_amount_avg_3_avg_6_prior,
    second_days_since_prev_claim_date,
    first_least_seq_no_approved_amount,
    rows_in_first_policy_start_date_month_seq_no
FROM sequenced
WHERE second_days_since_prev_claim_date >= 50
