WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount
    FROM claims
    WHERE claimed_amount > 4868.7
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount DESC, id) AS claimed_amount_change,
        LAG(claimed_amount, 2) OVER w_claim_type_adjuster_id_by_cl AS prev2_claimed_amount,
        SIZE(COLLECT_SET(claim_type) OVER w_status_policy_id) AS distinct_claim_type_in_status_policy_id,
        MIN(claimed_amount) OVER w_claim_type_adjuster_id AS claim_type_adjuster_id_min_claimed_amount,
        RANK() OVER (PARTITION BY claim_type, adjuster_id ORDER BY claimed_amount DESC, id) AS claimed_amount_rank,
        COUNT(*) OVER (PARTITION BY claim_type, adjuster_id) AS rows_in_claim_type_adjuster_id,
        DENSE_RANK() OVER (PARTITION BY claim_type, adjuster_id ORDER BY approved_amount_per_claimed_amount, id) AS approved_amount_per_claimed_amount_dense_rank,
        ROUND(CUME_DIST() OVER w_status_adjuster_id_by_approv, 3) AS approved_amount_per_claimed_amount_cume_dist,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_claim_type_adjuster_id, 0), 4) AS claimed_amount_share_of_claim_type_adjuster_id,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY status, adjuster_id ORDER BY approved_amount_per_claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount
    FROM prepared_claims
    WINDOW w_claim_type_adjuster_id_by_cl AS (PARTITION BY claim_type, adjuster_id ORDER BY claimed_amount DESC, id), w_status_policy_id AS (PARTITION BY status, policy_id), w_claim_type_adjuster_id AS (PARTITION BY claim_type, adjuster_id), w_status_adjuster_id_by_approv AS (PARTITION BY status, adjuster_id ORDER BY approved_amount_per_claimed_amount ASC)
)
SELECT
    id,
    adjuster_id,
    claim_type,
    claimed_amount,
    status,
    approved_amount_per_claimed_amount,
    prev2_claimed_amount,
    claimed_amount_rank,
    rows_in_claim_type_adjuster_id,
    approved_amount_per_claimed_amount_dense_rank,
    approved_amount_per_claimed_amount_cume_dist,
    claimed_amount_share_of_claim_type_adjuster_id,
    claimed_amount + last_claimed_amount AS claimed_amount_plus_last_claimed_amount,
    SUBSTRING(TRIM(status), 1, 4) AS status_prefix
FROM sequenced
ORDER BY id DESC, adjuster_id NULLS LAST
