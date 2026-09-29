WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status
    FROM claims
    WHERE claim_type <> 'fire'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id, claim_type ORDER BY claimed_amount ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_6_prior,
        FIRST_VALUE(claimed_amount) OVER (PARTITION BY status, policy_id ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount,
        MAX(claimed_amount) OVER (PARTITION BY claim_type) AS claim_type_max_claimed_amount,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_id, status ORDER BY approved_amount ASC), 3) AS approved_amount_percent_rank,
        FIRST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY status, policy_id ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY adjuster_id, status ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY policy_id, claim_type ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_adjuster_id, 0), 4) AS approved_amount_share_of_adjuster_id,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY adjuster_id), 0), 4) AS claimed_amount_share_of_adjuster_id,
        SIZE(COLLECT_SET(adjuster_id) OVER w_claim_type) AS distinct_adjuster_id_in_claim_type
    FROM scoped
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id), w_claim_type AS (PARTITION BY claim_type)
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_type,
    claimed_amount,
    approved_amount,
    status,
    approved_amount_avg_6_prior,
    first_claimed_amount,
    claim_type_max_claimed_amount,
    first_known_approved_amount,
    last_known_claimed_amount,
    claimed_amount_share_of_adjuster_id,
    distinct_adjuster_id_in_claim_type,
    CONCAT(claim_type, '#', CAST(id AS STRING)) AS claim_type_ref,
    COALESCE(approved_amount, 0.0) AS approved_amount_filled
FROM with_windows
