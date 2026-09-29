WITH recent_claims AS (
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
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE claim_type IN ('medical', 'theft')
),
combined_details AS (
    -- Attach adjuster attributes
    SELECT s.*, a.team AS adjuster_team
    FROM recent_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        greatest_claimed_amount_approved_amount,
        status_len,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY claim_type ORDER BY greatest_claimed_amount_approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        ROUND(AVG(status_len) OVER w_claim_type, 2) AS claim_type_avg_status_len,
        NTH_VALUE(status_len, 2) OVER (ORDER BY greatest_claimed_amount_approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_status_len,
        MAX(approved_amount) OVER (PARTITION BY claim_type) AS claim_type_max_approved_amount,
        ROUND(status_len - AVG(status_len) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), status), 2) AS status_len_vs_claim_date_month_status_avg,
        LAG(approved_amount, 2) OVER (PARTITION BY adjuster_team ORDER BY claim_date DESC NULLS FIRST, id) AS prev2_approved_amount,
        SIZE(COLLECT_SET(adjuster_id) OVER w_claim_date_month_status) AS distinct_adjuster_id_in_claim_date_month_status,
        LEAD(status_len) OVER (PARTITION BY adjuster_team ORDER BY claim_date DESC NULLS FIRST, id) AS next_status_len,
        SUM(approved_amount) OVER (PARTITION BY adjuster_team ORDER BY claim_date DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS approved_amount_sum_6_prior,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY claim_type)) AS distinct_adjuster_id_in_claim_type,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), status) AS rows_in_claim_date_month_status,
        NTILE(3) OVER (PARTITION BY claim_type ORDER BY greatest_claimed_amount_approved_amount DESC, id) AS greatest_claimed_amount_approved_amount_tercile,
        LEAD(status_len) OVER (ORDER BY greatest_claimed_amount_approved_amount, id) AS next_status_len_2,
        DENSE_RANK() OVER (PARTITION BY adjuster_team ORDER BY claim_date DESC NULLS FIRST) AS claim_date_dense_rank,
        ROUND(AVG(status_len) OVER (PARTITION BY adjuster_id ORDER BY approved_amount DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS status_len_avg_7
    FROM combined_details
    WINDOW w_claim_type AS (PARTITION BY claim_type), w_claim_date_month_status AS (PARTITION BY DATE_TRUNC('MONTH', claim_date), status)
),
tagged AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(status)) AS status_len_2
    FROM with_windows
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY claim_date_dense_rank, status_len_2) AS rows_in_claim_date_dense_rank_status_len_2,
        MAX(claim_type_avg_status_len) OVER w_claim_date_dense_rank_status AS claim_date_dense_rank_status_len_2_max_claim_typ,
        MIN(next_status_len) OVER w_claim_date_dense_rank_status AS claim_date_dense_rank_status_len_2_min_next_stat,
        ROUND(second_approved_amount / NULLIF(SUM(second_approved_amount) OVER (PARTITION BY claim_date_dense_rank, status_len_2), 0), 4) AS second_approved_amount_share_of_claim_date_dense
    FROM tagged
    WINDOW w_claim_date_dense_rank_status AS (PARTITION BY claim_date_dense_rank, status_len_2)
)
SELECT
    id,
    policy_id,
    claimed_amount,
    approved_amount,
    status,
    greatest_claimed_amount_approved_amount,
    claim_type_avg_status_len,
    second_status_len,
    status_len_vs_claim_date_month_status_avg,
    prev2_approved_amount,
    next_status_len,
    approved_amount_sum_6_prior,
    distinct_adjuster_id_in_claim_type,
    rows_in_claim_date_month_status,
    greatest_claimed_amount_approved_amount_tercile,
    next_status_len_2,
    claim_date_dense_rank,
    status_len_avg_7,
    status_len_2,
    claim_date_dense_rank_status_len_2_max_claim_typ,
    claim_date_dense_rank_status_len_2_min_next_stat,
    second_approved_amount_share_of_claim_date_dense
FROM windowed
ORDER BY second_status_len, id
LIMIT 5
