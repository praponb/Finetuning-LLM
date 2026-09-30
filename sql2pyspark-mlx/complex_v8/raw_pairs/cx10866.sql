WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        DATE_TRUNC('WEEK', reported_at) AS reported_at_week
    FROM claims
    WHERE status IN ('PAID', 'REJECTED', 'OPEN')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(claimed_amount, 3, 0) OVER w_adjuster_id_by_reported_at_w AS next3_claimed_amount,
        MIN(approved_amount) OVER (PARTITION BY claim_type) AS claim_type_min_approved_amount,
        ROW_NUMBER() OVER (PARTITION BY claim_type, adjuster_id ORDER BY reported_at, id) AS reported_at_seq,
        DENSE_RANK() OVER (ORDER BY reported_at DESC, id) AS reported_at_dense_rank,
        ROW_NUMBER() OVER w_all_by_reported_at AS seq_no,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY claim_type, adjuster_id ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_policy_id_adjuster_id, 0), 4) AS claimed_amount_share_of_policy_id_adjuster_id
    FROM scoped
    WINDOW w_adjuster_id_by_reported_at_w AS (PARTITION BY adjuster_id ORDER BY reported_at_week DESC, id), w_all_by_reported_at AS (ORDER BY reported_at DESC, id), w_policy_id_adjuster_id AS (PARTITION BY policy_id, adjuster_id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at_week,
        next3_claimed_amount,
        reported_at_seq,
        reported_at_dense_rank,
        seq_no,
        second_approved_amount,
        claimed_amount_share_of_policy_id_adjuster_id,
        NTILE(5) OVER (PARTITION BY seq_no ORDER BY reported_at_dense_rank DESC, id) AS reported_at_dense_rank_quintile,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)), 2) AS claimed_amount_vs_claim_date_month_avg,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY seq_no, MONTH(claim_date))) AS distinct_adjuster_id_in_seq_no_claim_date_month,
        NTH_VALUE(approved_amount, 2) OVER (ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount_2,
        LAST_VALUE(seq_no) OVER (PARTITION BY policy_id ORDER BY claimed_amount_share_of_policy_id_adjuster_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_seq_no,
        SUM(second_approved_amount) OVER w_seq_no_claim_date_month AS seq_no_claim_date_month_sum_second_approved_amou,
        NTH_VALUE(next3_claimed_amount, 2) OVER (PARTITION BY policy_id ORDER BY claimed_amount_share_of_policy_id_adjuster_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next3_claimed_amount,
        ROUND((UNIX_TIMESTAMP(reported_at_week) - UNIX_TIMESTAMP(LAG(reported_at_week) OVER w_adjuster_id_seq_no_by_second)) / 60, 1) AS minutes_since_prev_reported_at_week
    FROM sequenced
    WINDOW w_seq_no_claim_date_month AS (PARTITION BY seq_no, MONTH(claim_date)), w_adjuster_id_seq_no_by_second AS (PARTITION BY adjuster_id, seq_no ORDER BY second_approved_amount DESC, id)
)
-- Final projection
SELECT
    id,
    claim_type,
    reported_at_week,
    reported_at_seq,
    second_approved_amount,
    claimed_amount_share_of_policy_id_adjuster_id,
    reported_at_dense_rank_quintile,
    claimed_amount_vs_claim_date_month_avg,
    distinct_adjuster_id_in_seq_no_claim_date_month,
    second_approved_amount_2,
    seq_no_claim_date_month_sum_second_approved_amou,
    minutes_since_prev_reported_at_week,
    COALESCE(approved_amount, 0.0) AS approved_amount_filled,
    LEAST(next3_claimed_amount, last_seq_no) AS least_next3_claimed_amount_last_seq_no
FROM metrics
WHERE claim_date BETWEEN DATE '2024-02-23' AND DATE '2025-11-12'
