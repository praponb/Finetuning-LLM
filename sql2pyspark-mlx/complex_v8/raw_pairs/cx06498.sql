WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at,
        CASE WHEN approved_amount >= 14668.0 THEN 'large' WHEN approved_amount >= 4554.8 THEN 'medium' ELSE 'small' END AS approved_amount_tier
    FROM claims
    WHERE approved_amount >= 0.0
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(claimed_amount) OVER (PARTITION BY adjuster_id, approved_amount_tier ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_so_far,
        DENSE_RANK() OVER w_adjuster_id_approved_amount_2 AS reported_at_dense_rank,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_id, status)) AS distinct_adjuster_id_in_policy_id_status
    FROM cleaned
    WINDOW w_adjuster_id_approved_amount_2 AS (PARTITION BY adjuster_id, approved_amount_tier ORDER BY reported_at)
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE reported_at_dense_rank <= 2
)
SELECT
    id,
    adjuster_id,
    claimed_amount,
    status,
    reported_at,
    approved_amount_tier,
    max_claimed_amount_so_far,
    reported_at_dense_rank
FROM leaders
WHERE reported_at_dense_rank <= 8
ORDER BY approved_amount_tier
