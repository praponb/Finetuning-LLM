WITH valid_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        DAYOFWEEK(claim_date) AS claim_date_dayofweek
    FROM claims
    WHERE adjuster_id <= 7
),
joined_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.reported_at,
        s.claim_date_dayofweek,
        p.status AS policy_status,
        p.holder_id AS policy_holder_id,
        p.product AS policy_product
    FROM valid_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        reported_at,
        policy_product,
        MAX(approved_amount) OVER (PARTITION BY adjuster_id) AS adjuster_id_max_approved_amount,
        ROUND(CUME_DIST() OVER w_claim_type_policy_product_by, 3) AS claim_date_cume_dist,
        SUM(approved_amount) OVER (PARTITION BY claim_type, policy_product ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_approved_amount,
        RANK() OVER w_policy_holder_id_adjuster_id AS reported_at_rank,
        ROUND(approved_amount - AVG(approved_amount) OVER w_policy_holder_id_policy_id, 2) AS approved_amount_vs_policy_holder_id_policy_id_av,
        ROUND(AVG(claim_date_dayofweek) OVER (PARTITION BY adjuster_id), 2) AS adjuster_id_avg_claim_date_dayofweek,
        SUM(approved_amount) OVER (PARTITION BY claim_type, policy_product ORDER BY claim_date DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS approved_amount_sum_2_prior,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_holder_id, adjuster_id ORDER BY reported_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_2,
        FIRST_VALUE(claim_date) OVER (PARTITION BY claim_type, policy_product ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        ROUND(CUME_DIST() OVER w_policy_product_policy_id_by, 3) AS reported_at_cume_dist
    FROM joined_policy
    WINDOW w_claim_type_policy_product_by AS (PARTITION BY claim_type, policy_product ORDER BY claim_date DESC, id), w_policy_holder_id_adjuster_id AS (PARTITION BY policy_holder_id, adjuster_id ORDER BY reported_at), w_policy_holder_id_policy_id AS (PARTITION BY policy_holder_id, policy_id), w_policy_product_policy_id_by AS (PARTITION BY policy_product, policy_id ORDER BY reported_at)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM with_trends
    WHERE reported_at_rank = 1
      AND running_approved_amount < 425000.0
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    reported_at,
    adjuster_id_max_approved_amount,
    approved_amount_vs_policy_holder_id_policy_id_av,
    adjuster_id_avg_claim_date_dayofweek,
    approved_amount_sum_2_prior,
    reported_at_cume_dist,
    DAYOFWEEK(reported_at) AS reported_at_dayofweek,
    CONCAT(policy_product, '#', CAST(id AS STRING)) AS policy_product_ref
FROM first_rows
WHERE adjuster_id_max_approved_amount > 100000.0
ORDER BY adjuster_id_max_approved_amount, adjuster_id_avg_claim_date_dayofweek
