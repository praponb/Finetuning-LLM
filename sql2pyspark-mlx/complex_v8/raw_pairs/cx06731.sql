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
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount
    FROM claims
    WHERE (claim_type <> 'flood' OR status IS NOT NULL)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_claimed_amount_ties AS claimed_amount_dense_rank,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY status, claim_type), 0), 4) AS approved_amount_share_of_status_claim_type,
        FIRST_VALUE(reported_at) OVER (ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at,
        FIRST_VALUE(approved_amount) IGNORE NULLS OVER (ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount,
        ROW_NUMBER() OVER w_all_by_claimed_amount AS seq_no
    FROM filtered
    WINDOW w_all_by_claimed_amount_ties AS (ORDER BY claimed_amount ASC NULLS LAST), w_all_by_claimed_amount AS (ORDER BY claimed_amount ASC NULLS LAST, id)
)
-- Subtotals with ROLLUP
SELECT
    adjuster_id,
    policy_id,
    ROUND(SUM(claimed_amount) / NULLIF(SUM(claimed_amount_dense_rank), 0), 3) AS claimed_amount_to_claimed_amount_dense_rank_rati,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list,
    MAX(claim_date) AS last_claim_date
FROM with_windows
GROUP BY ROLLUP(adjuster_id, policy_id)
ORDER BY adjuster_id NULLS LAST, policy_id NULLS LAST
