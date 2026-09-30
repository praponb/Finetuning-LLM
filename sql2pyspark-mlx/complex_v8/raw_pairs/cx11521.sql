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
        reported_at
    FROM claims
    WHERE status = 'REJECTED'
      AND claim_type = 'delay'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        ROUND(CUME_DIST() OVER (ORDER BY claim_date), 3) AS claim_date_cume_dist,
        MAX(approved_amount) OVER (PARTITION BY status) AS status_max_approved_amount
    FROM filtered
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        claim_date_cume_dist,
        status_max_approved_amount,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        COALESCE(claim_date_cume_dist, 0.0) AS claim_date_cume_dist_filled
    FROM sequenced
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(approved_amount_filled) IGNORE NULLS OVER (PARTITION BY claim_type ORDER BY approved_amount_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount_filled,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY approved_amount_filled DESC), 3) AS approved_amount_filled_percent_rank,
        LAG(claim_date_cume_dist_filled, 2) OVER (ORDER BY claim_date_cume_dist DESC, id) AS prev2_claim_date_cume_dist_filled,
        DENSE_RANK() OVER (PARTITION BY claim_type ORDER BY approved_amount_filled DESC, id) AS approved_amount_filled_dense_rank
    FROM scored
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, status_max_approved_amount, approved_amount, approved_amount_filled, prev2_claim_date_cume_dist_filled
    FROM windowed
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (status_max_approved_amount, approved_amount, approved_amount_filled, prev2_claim_date_cume_dist_filled)
) unp
