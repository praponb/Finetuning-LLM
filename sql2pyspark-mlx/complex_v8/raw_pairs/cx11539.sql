WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CONCAT_WS('-', status, claim_type) AS status_claim_type_label
    FROM claims
    WHERE policy_id <= 7
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        claim_type,
        approved_amount,
        status,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY claim_type, adjuster_id), 0), 4) AS approved_amount_share_of_claim_type_adjuster_id,
        NTILE(5) OVER (ORDER BY reported_at, id) AS reported_at_quintile,
        MAX(approved_amount) OVER (PARTITION BY claim_type, adjuster_id) AS claim_type_adjuster_id_max_approved_amount,
        MIN(approved_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_approved_amount_so_far,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY claim_type)) AS distinct_status_in_claim_type,
        DENSE_RANK() OVER (ORDER BY reported_at, id) AS reported_at_dense_rank,
        NTH_VALUE(approved_amount, 2) OVER (ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY claim_type), 2) AS approved_amount_vs_claim_type_avg,
        MAX(approved_amount) OVER (PARTITION BY claim_type) AS claim_type_max_approved_amount,
        LAST_VALUE(claimed_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        DENSE_RANK() OVER (ORDER BY approved_amount, id) AS approved_amount_dense_rank
    FROM valid
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        claim_type,
        approved_amount,
        status,
        approved_amount_share_of_claim_type_adjuster_id,
        reported_at_quintile,
        claim_type_adjuster_id_max_approved_amount,
        min_approved_amount_so_far,
        distinct_status_in_claim_type,
        second_approved_amount,
        approved_amount_vs_claim_type_avg,
        claim_type_max_approved_amount,
        last_claimed_amount,
        approved_amount_dense_rank
    FROM metrics
    WHERE reported_at_dense_rank <= 2
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        approved_amount,
        status,
        approved_amount_share_of_claim_type_adjuster_id,
        reported_at_quintile,
        claim_type_adjuster_id_max_approved_amount,
        min_approved_amount_so_far,
        distinct_status_in_claim_type,
        approved_amount_vs_claim_type_avg,
        last_claimed_amount,
        ROW_NUMBER() OVER w_all_by_distinct_status_in_cl AS seq_no,
        SUM(second_approved_amount) OVER (PARTITION BY reported_at_quintile ORDER BY second_approved_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_second_approved_amount,
        last_claimed_amount - LAG(last_claimed_amount) OVER (ORDER BY distinct_status_in_claim_type DESC, id) AS last_claimed_amount_change,
        SUM(approved_amount) OVER (PARTITION BY approved_amount_dense_rank, status) AS approved_amount_dense_rank_status_sum_approved_a
    FROM top_rows
    WINDOW w_all_by_distinct_status_in_cl AS (ORDER BY distinct_status_in_claim_type DESC, id)
)
SELECT
    id,
    approved_amount,
    status,
    approved_amount_share_of_claim_type_adjuster_id,
    reported_at_quintile,
    claim_type_adjuster_id_max_approved_amount,
    min_approved_amount_so_far,
    distinct_status_in_claim_type,
    approved_amount_vs_claim_type_avg,
    running_second_approved_amount,
    CASE WHEN last_claimed_amount >= 29336.0 THEN 'large' WHEN last_claimed_amount >= 13635.1 THEN 'medium' ELSE 'small' END AS last_claimed_amount_tier,
    ROUND(seq_no / NULLIF(reported_at_quintile, 0), 3) AS seq_no_per_reported_at_quintile
FROM metrics_2
WHERE status IN ('OPEN', 'APPROVED', 'PAID')
ORDER BY running_second_approved_amount DESC
