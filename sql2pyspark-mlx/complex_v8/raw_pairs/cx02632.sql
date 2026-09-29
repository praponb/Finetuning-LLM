WITH base_claims AS (
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
        CONCAT(status, '#', CAST(adjuster_id AS STRING)) AS status_ref
    FROM claims
    WHERE claim_type IS NOT NULL
      AND policy_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(approved_amount) OVER w_status_claim_type AS status_claim_type_min_approved_amount,
        MIN(approved_amount) OVER (PARTITION BY YEAR(reported_at) ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_approved_amount_so_far,
        MIN(claimed_amount) OVER (PARTITION BY status, policy_id) AS status_policy_id_min_claimed_amount,
        LEAD(claim_date) OVER w_status_by_approved_amount AS next_claim_date,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_status_policy_id, 0), 4) AS claimed_amount_share_of_status_policy_id,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_status_claim_type, 2) AS claimed_amount_vs_status_claim_type_avg,
        LAG(claim_date) OVER (PARTITION BY policy_id, adjuster_id ORDER BY reported_at DESC, id) AS prev_claim_date,
        MAX(approved_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_max_3,
        FIRST_VALUE(claimed_amount) OVER (PARTITION BY status ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY status, policy_id)) AS distinct_adjuster_id_in_status_policy_id,
        LEAD(reported_at, 3) OVER (PARTITION BY YEAR(reported_at) ORDER BY reported_at, id) AS next3_reported_at
    FROM base_claims
    WINDOW w_status_claim_type AS (PARTITION BY status, claim_type), w_status_by_approved_amount AS (PARTITION BY status ORDER BY approved_amount DESC, id), w_status_policy_id AS (PARTITION BY status, policy_id)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        status_claim_type_min_approved_amount,
        status_policy_id_min_claimed_amount,
        claimed_amount_share_of_status_policy_id,
        claimed_amount_vs_status_claim_type_avg,
        approved_amount_max_3,
        distinct_adjuster_id_in_status_policy_id,
        LAST_VALUE(first_claimed_amount) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_claimed_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_claim_type, 2) AS claimed_amount_vs_claim_type_avg,
        DENSE_RANK() OVER (PARTITION BY policy_id ORDER BY reported_at DESC) AS reported_at_dense_rank,
        LAG(min_approved_amount_so_far) OVER (PARTITION BY status ORDER BY claimed_amount_share_of_status_policy_id, id) AS prev_min_approved_amount_so_far,
        COUNT(*) OVER (PARTITION BY claim_type) AS rows_in_claim_type
    FROM with_windows
    WINDOW w_claim_type AS (PARTITION BY claim_type)
)
-- Final projection
SELECT
    id,
    policy_id,
    status_claim_type_min_approved_amount,
    status_policy_id_min_claimed_amount,
    claimed_amount_share_of_status_policy_id,
    claimed_amount_vs_status_claim_type_avg,
    approved_amount_max_3,
    distinct_adjuster_id_in_status_policy_id,
    claimed_amount_vs_claim_type_avg,
    reported_at_dense_rank,
    prev_min_approved_amount_so_far,
    rows_in_claim_type,
    SUBSTRING(TRIM(status), 1, 2) AS status_prefix
FROM sequenced
ORDER BY id DESC NULLS FIRST
