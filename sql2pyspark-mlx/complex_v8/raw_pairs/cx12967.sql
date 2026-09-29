WITH filtered AS (
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
        CONCAT(claim_type, '#', CAST(id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE claimed_amount <= 44991.5
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claim_type,
        approved_amount,
        status,
        MIN(approved_amount) OVER (PARTITION BY status ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_approved_amount_so_far,
        NTILE(5) OVER w_status_by_approved_amount AS approved_amount_quintile,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY status ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY policy_id ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        DENSE_RANK() OVER (PARTITION BY policy_id ORDER BY approved_amount DESC) AS approved_amount_dense_rank,
        LAG(claimed_amount) OVER w_status_by_approved_amount AS prev_claimed_amount
    FROM filtered
    WINDOW w_status_by_approved_amount AS (PARTITION BY status ORDER BY approved_amount, id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE approved_amount_dense_rank = 1
),
totals AS (
    -- Group-level totals
    SELECT
        policy_id,
        approved_amount_dense_rank,
        claim_type,
        MAX(prev_claimed_amount) AS max_prev_claimed_amount,
        ROUND(AVG(last_known_claimed_amount), 2) AS avg_last_known_claimed_amount
    FROM top_per_group
    GROUP BY policy_id, approved_amount_dense_rank, claim_type
    HAVING AVG(prev_claimed_amount) > 11235.6
)
SELECT
    policy_id,
    approved_amount_dense_rank,
    claim_type,
    max_prev_claimed_amount,
    COALESCE(max_prev_claimed_amount, 0.0) AS max_prev_claimed_amount_filled
FROM totals
ORDER BY max_prev_claimed_amount_filled DESC, policy_id, approved_amount_dense_rank, claim_type
LIMIT 10
