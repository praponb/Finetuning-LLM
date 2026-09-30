WITH scoped_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        LEAD(claimed_amount) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount_filled DESC, id) AS next_claimed_amount,
        ROUND(AVG(claimed_amount_filled) OVER (PARTITION BY policy_id), 2) AS policy_id_avg_claimed_amount_filled
    FROM scoped_claims
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claim_type IN ('delay', 'theft', 'fire')
              AND MONTH(c.reported_at) IN (1, 3)
        )
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(claimed_amount) OVER (PARTITION BY status, adjuster_id) AS status_adjuster_id_sum_claimed_amount,
        FIRST_VALUE(claimed_amount_filled) OVER (ORDER BY claimed_amount_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount_filled,
        RANK() OVER w_all_by_claimed_amount_filled_2 AS claimed_amount_filled_rank
    FROM filtered
    WINDOW w_all_by_claimed_amount_filled_2 AS (ORDER BY claimed_amount_filled DESC)
)
-- Subtotals with ROLLUP
SELECT
    adjuster_id,
    claimed_amount_filled_rank,
    COUNT(DISTINCT policy_id) AS distinct_policy_ids
FROM sequenced_2
GROUP BY ROLLUP(adjuster_id, claimed_amount_filled_rank)
ORDER BY adjuster_id NULLS LAST, claimed_amount_filled_rank NULLS LAST
