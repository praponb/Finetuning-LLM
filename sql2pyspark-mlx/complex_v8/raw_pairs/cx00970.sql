WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM claims
    WHERE claim_type IN ('flood', 'delay')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(approved_amount) OVER (PARTITION BY adjuster_id, claim_type) AS adjuster_id_claim_type_sum_approved_amount,
        DENSE_RANK() OVER w_adjuster_id_by_approved_amou AS approved_amount_bucket_dense_rank,
        MAX(claimed_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_so_far
    FROM prepared_claims
    WINDOW w_adjuster_id_by_approved_amou AS (PARTITION BY adjuster_id ORDER BY approved_amount_bucket DESC, id)
),
combined_details AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.approved_amount_bucket,
        s.max_claimed_amount_so_far,
        p.premium AS policy_premium,
        p.holder_id AS policy_holder_id,
        p.end_date AS policy_end_date,
        po.risk_score AS holder_risk_score
    FROM with_windows s
    LEFT JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        approved_amount_bucket,
        policy_holder_id,
        policy_end_date,
        RANK() OVER (PARTITION BY policy_id, policy_holder_id ORDER BY policy_end_date) AS policy_end_date_rank,
        MIN(approved_amount) OVER (PARTITION BY policy_id) AS policy_id_min_approved_amount,
        NTH_VALUE(holder_risk_score, 2) OVER (PARTITION BY policy_id, policy_holder_id ORDER BY policy_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_risk_score,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id, policy_holder_id ORDER BY policy_end_date), 3) AS policy_end_date_cume_dist,
        LAST_VALUE(max_claimed_amount_so_far) IGNORE NULLS OVER (PARTITION BY policy_id, policy_holder_id ORDER BY policy_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_max_claimed_amount_so_far
    FROM combined_details
)
-- Final projection
SELECT
    id,
    policy_id,
    approved_amount_bucket,
    policy_end_date,
    last_known_max_claimed_amount_so_far,
    DATE_FORMAT(policy_end_date, 'yyyy-MM') AS policy_end_date_label
FROM sequenced
WHERE policy_id IS NOT NULL
ORDER BY policy_id DESC, policy_end_date DESC, id
LIMIT 10
