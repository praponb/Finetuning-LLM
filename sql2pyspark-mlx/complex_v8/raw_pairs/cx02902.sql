WITH scoped_claims AS (
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
        LEAST(claimed_amount, approved_amount) AS least_claimed_amount_approved_amount,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM claims
    WHERE status = 'REJECTED'
      AND claimed_amount <= 42447.2
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        least_claimed_amount_approved_amount,
        LAST_VALUE(claimed_amount_bucket) IGNORE NULLS OVER (ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount_bucket,
        DENSE_RANK() OVER w_all_by_claimed_amount_ties AS claimed_amount_dense_rank,
        ROUND(CUME_DIST() OVER w_reported_at_to_date_adjuster, 3) AS claimed_amount_bucket_cume_dist,
        LAST_VALUE(reported_at) OVER (ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at,
        SUM(claimed_amount) OVER w_adjuster_id_policy_id AS adjuster_id_policy_id_sum_claimed_amount,
        MIN(claimed_amount) OVER w_policy_id AS policy_id_min_claimed_amount,
        ROW_NUMBER() OVER (ORDER BY approved_amount, id) AS rn,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY TO_DATE(reported_at), adjuster_id ORDER BY claimed_amount_bucket DESC, id) AS claimed_amount_change,
        MIN(approved_amount) OVER (PARTITION BY policy_id, status) AS policy_id_status_min_approved_amount
    FROM scoped_claims
    WINDOW w_all_by_claimed_amount_ties AS (ORDER BY claimed_amount ASC), w_reported_at_to_date_adjuster AS (PARTITION BY TO_DATE(reported_at), adjuster_id ORDER BY claimed_amount_bucket DESC, id), w_adjuster_id_policy_id AS (PARTITION BY adjuster_id, policy_id), w_policy_id AS (PARTITION BY policy_id)
)
-- Final projection
SELECT
    id,
    claim_date,
    claim_type,
    claimed_amount,
    approved_amount,
    least_claimed_amount_approved_amount,
    last_known_claimed_amount_bucket,
    claimed_amount_dense_rank,
    claimed_amount_bucket_cume_dist,
    last_reported_at,
    adjuster_id_policy_id_sum_claimed_amount,
    rn,
    policy_id_status_min_approved_amount
FROM windowed
ORDER BY rn DESC, claimed_amount_dense_rank DESC
