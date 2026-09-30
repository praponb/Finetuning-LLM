WITH scoped AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE (claimed_amount > 19337.6 OR approved_amount <= 15857.9)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY adjuster_id, policy_id), 0), 4) AS claimed_amount_share_of_adjuster_id_policy_id,
        COUNT(*) OVER (ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_id, policy_id), 2) AS adjuster_id_policy_id_avg_claimed_amount,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY reported_at) AS reported_at_dense_rank,
        LAG(claimed_amount, 3) OVER w_all_by_claimed_amount AS prev3_claimed_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY adjuster_id, policy_id), 2) AS claimed_amount_vs_adjuster_id_policy_id_avg,
        ROUND(AVG(approved_amount) OVER w_policy_id, 2) AS policy_id_avg_approved_amount
    FROM scoped
    WINDOW w_all_by_claimed_amount AS (ORDER BY claimed_amount ASC, id), w_policy_id AS (PARTITION BY policy_id)
),
first_rows AS (
    -- Top 3 per group
    SELECT *
    FROM with_windows
    WHERE reported_at_dense_rank <= 3
      AND policy_id IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(reported_at_dense_rank) OVER (PARTITION BY reported_at_dense_rank, claim_type ORDER BY claimed_amount_share_of_adjuster_id_policy_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_reported_at_dense_rank_so_far,
        RANK() OVER (PARTITION BY reported_at_dense_rank, TO_DATE(reported_at) ORDER BY claimed_amount DESC) AS claimed_amount_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY reported_at_dense_rank, TO_DATE(reported_at) ORDER BY claimed_amount DESC, id), 3) AS claimed_amount_percent_rank,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id_2
    FROM first_rows
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_type,
    claimed_amount,
    status,
    reported_at,
    claimed_amount_share_of_adjuster_id_policy_id,
    running_count,
    adjuster_id_policy_id_avg_claimed_amount,
    rows_in_policy_id,
    reported_at_dense_rank,
    prev3_claimed_amount,
    claimed_amount_vs_adjuster_id_policy_id_avg,
    claimed_amount_percent_rank,
    rows_in_policy_id_2
FROM windowed
ORDER BY reported_at_dense_rank
