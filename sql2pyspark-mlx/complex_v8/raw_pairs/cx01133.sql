WITH filtered AS (
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
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        reported_at + INTERVAL 2 HOURS AS reported_at_plus_2h
    FROM claims
    WHERE policy_id IS NOT NULL
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        approved_amount,
        status,
        claimed_amount_filled,
        reported_at_plus_2h,
        SUM(approved_amount) OVER (PARTITION BY claim_type) AS claim_type_sum_approved_amount,
        MAX(claimed_amount_filled) OVER (PARTITION BY adjuster_id, policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_filled_so_far,
        MAX(claimed_amount) OVER (ORDER BY claimed_amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS claimed_amount_max_6_prior,
        ROW_NUMBER() OVER (PARTITION BY adjuster_id, policy_id ORDER BY approved_amount ASC NULLS LAST, id) AS seq_no,
        COUNT(*) OVER (PARTITION BY claim_type) AS rows_in_claim_type,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id, status ORDER BY claimed_amount_filled DESC NULLS FIRST, id), 3) AS claimed_amount_filled_percent_rank,
        SUM(claimed_amount) OVER (PARTITION BY policy_id, status ORDER BY claimed_amount_filled DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        MAX(claimed_amount_filled) OVER (PARTITION BY adjuster_id, claim_type) AS adjuster_id_claim_type_max_claimed_amount_filled,
        DENSE_RANK() OVER (ORDER BY claimed_amount) AS claimed_amount_dense_rank,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_id, policy_id ORDER BY claim_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_3_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_id, policy_id ORDER BY approved_amount ASC NULLS LAST, id), 3) AS approved_amount_percent_rank,
        LAG(claimed_amount) OVER (PARTITION BY adjuster_id, policy_id ORDER BY claim_date, id) AS prev_claimed_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY claim_type), 2) AS claimed_amount_vs_claim_type_avg
    FROM filtered
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        policy_id,
        claim_date,
        approved_amount,
        status,
        claimed_amount_filled,
        reported_at_plus_2h,
        claim_type_sum_approved_amount,
        max_claimed_amount_filled_so_far,
        claimed_amount_max_6_prior,
        seq_no,
        rows_in_claim_type,
        claimed_amount_filled_percent_rank,
        running_claimed_amount,
        adjuster_id_claim_type_max_claimed_amount_filled,
        claimed_amount_avg_3_prior,
        approved_amount_percent_rank,
        prev_claimed_amount,
        claimed_amount_vs_claim_type_avg
    FROM sequenced
    WHERE claimed_amount_dense_rank <= 2
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        approved_amount,
        status,
        claimed_amount_filled,
        claim_type_sum_approved_amount,
        max_claimed_amount_filled_so_far,
        claimed_amount_max_6_prior,
        seq_no,
        claimed_amount_filled_percent_rank,
        running_claimed_amount,
        adjuster_id_claim_type_max_claimed_amount_filled,
        approved_amount_percent_rank,
        prev_claimed_amount,
        claimed_amount_vs_claim_type_avg,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY status)) AS distinct_policy_id_in_status,
        MAX(prev_claimed_amount) OVER (PARTITION BY status) AS status_max_prev_claimed_amount,
        SUM(claimed_amount_avg_3_prior) OVER (PARTITION BY policy_id ORDER BY reported_at_plus_2h DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount_avg_3_prior,
        MIN(approved_amount_percent_rank) OVER (PARTITION BY policy_id ORDER BY reported_at_plus_2h DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_approved_amount_percent_rank_so_far
    FROM top_per_group
)
-- Final projection
SELECT
    id,
    approved_amount,
    claimed_amount_filled,
    max_claimed_amount_filled_so_far,
    claimed_amount_filled_percent_rank,
    running_claimed_amount,
    adjuster_id_claim_type_max_claimed_amount_filled,
    approved_amount_percent_rank,
    claimed_amount_vs_claim_type_avg,
    status_max_prev_claimed_amount
FROM sequenced_2
ORDER BY claimed_amount_vs_claim_type_avg, approved_amount
