WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        reported_at,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount
    FROM claims
    WHERE claim_type NOT IN ('theft', 'delay')
),
enriched_policy AS (
    -- Join policies
    SELECT s.*, p.product AS policy_product, p.start_date AS policy_start_date
    FROM filtered_claims s
    JOIN policies p ON p.id = s.policy_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (ORDER BY reported_at ASC) AS reported_at_rank,
        LEAD(policy_start_date) OVER (ORDER BY reported_at, id) AS next_policy_start_date,
        DENSE_RANK() OVER (ORDER BY reported_at) AS reported_at_dense_rank,
        COUNT(*) OVER (PARTITION BY adjuster_id, policy_product) AS rows_in_adjuster_id_policy_product,
        LAG(approved_amount, 2) OVER (PARTITION BY policy_product ORDER BY reported_at DESC NULLS FIRST, id) AS prev2_approved_amount,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_type ORDER BY least_approved_amount_claimed_amount ASC, id), 3) AS least_approved_amount_claimed_amount_cume_dist,
        DENSE_RANK() OVER (PARTITION BY policy_product ORDER BY reported_at DESC NULLS FIRST) AS reported_at_dense_rank_2,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_product), 0), 4) AS approved_amount_share_of_policy_product,
        DENSE_RANK() OVER w_claim_type_by_least_approved_2 AS least_approved_amount_claimed_amount_dense_rank,
        NTH_VALUE(approved_amount, 2) OVER (ORDER BY reported_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        MIN(approved_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_approved_amount_so_far,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY adjuster_id, policy_product), 0), 4) AS approved_amount_share_of_adjuster_id_policy_prod,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (ORDER BY reported_at ASC, id))) / 60, 1) AS minutes_since_prev_reported_at
    FROM enriched_policy
    WINDOW w_claim_type_by_least_approved_2 AS (PARTITION BY claim_type ORDER BY least_approved_amount_claimed_amount ASC)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    reported_at,
    least_approved_amount_claimed_amount,
    policy_product,
    policy_start_date,
    reported_at_rank,
    next_policy_start_date,
    reported_at_dense_rank,
    rows_in_adjuster_id_policy_product,
    prev2_approved_amount,
    least_approved_amount_claimed_amount_cume_dist,
    reported_at_dense_rank_2,
    approved_amount_share_of_policy_product,
    least_approved_amount_claimed_amount_dense_rank,
    second_approved_amount,
    approved_amount_share_of_adjuster_id_policy_prod,
    DATE_FORMAT(policy_start_date, 'yyyy-MM-dd') AS policy_start_date_label
FROM with_windows
ORDER BY least_approved_amount_claimed_amount_dense_rank, next_policy_start_date, id
LIMIT 10
