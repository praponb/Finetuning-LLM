WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        CONCAT_WS(' / ', status, claim_type) AS status_claim_type_label
    FROM claims
    WHERE approved_amount >= 0.0
),
enriched AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.status,
        s.reported_at,
        s.status_claim_type_label,
        p.end_date AS policy_end_date,
        p.product AS policy_product,
        p.holder_id AS policy_holder_id,
        po.joined_on AS holder_joined_on
    FROM filtered_claims s
    JOIN policies p ON p.id = s.policy_id AND p.premium >= 2033.8
    LEFT JOIN policyholders po ON po.id = p.holder_id
    WHERE p.holder_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        reported_at,
        status_claim_type_label,
        policy_end_date,
        policy_product,
        policy_holder_id,
        holder_joined_on,
        RANK() OVER (PARTITION BY policy_holder_id ORDER BY policy_end_date DESC) AS policy_end_date_rank,
        LAG(reported_at) OVER (PARTITION BY policy_holder_id ORDER BY holder_joined_on, id) AS prev_reported_at,
        ROUND(CUME_DIST() OVER w_claim_type_policy_holder_id, 3) AS holder_joined_on_cume_dist,
        DENSE_RANK() OVER w_all_by_holder_joined_on AS holder_joined_on_dense_rank,
        NTH_VALUE(holder_joined_on, 2) OVER (PARTITION BY claim_type, policy_holder_id ORDER BY holder_joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_joined_on,
        FIRST_VALUE(holder_joined_on) OVER (PARTITION BY policy_holder_id ORDER BY holder_joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_holder_joined_on,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY policy_holder_id ORDER BY policy_end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY policy_holder_id ORDER BY holder_joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at,
        LAG(reported_at, 3) OVER (PARTITION BY policy_holder_id ORDER BY policy_end_date DESC, id) AS prev3_reported_at
    FROM enriched
    WINDOW w_claim_type_policy_holder_id AS (PARTITION BY claim_type, policy_holder_id ORDER BY holder_joined_on, id), w_all_by_holder_joined_on AS (ORDER BY holder_joined_on DESC NULLS FIRST, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_id,
        policy_end_date,
        policy_product,
        policy_holder_id,
        holder_joined_on,
        prev_reported_at,
        holder_joined_on_dense_rank,
        second_holder_joined_on,
        first_holder_joined_on,
        second_reported_at,
        first_known_reported_at,
        prev3_reported_at,
        ROUND(holder_joined_on_dense_rank / NULLIF(SUM(holder_joined_on_dense_rank) OVER (PARTITION BY MONTH(holder_joined_on)), 0), 4) AS holder_joined_on_dense_rank_share_of_holder_join,
        LAST_VALUE(second_reported_at) IGNORE NULLS OVER (ORDER BY holder_joined_on_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_second_reported_at,
        ROUND(AVG(policy_end_date_rank) OVER (PARTITION BY MONTH(holder_joined_on)), 2) AS holder_joined_on_month_avg_policy_end_date_rank,
        ROW_NUMBER() OVER (ORDER BY first_known_reported_at, id) AS rn,
        LAST_VALUE(holder_joined_on_cume_dist) IGNORE NULLS OVER (ORDER BY holder_joined_on_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_holder_joined_on_cume_dist,
        MIN(holder_joined_on_cume_dist) OVER (PARTITION BY MONTH(holder_joined_on)) AS holder_joined_on_month_min_holder_joined_on_cume,
        MAX(holder_joined_on_dense_rank) OVER (PARTITION BY MONTH(holder_joined_on)) AS holder_joined_on_month_max_holder_joined_on_dens
    FROM with_windows
)
-- Final projection
SELECT
    id,
    adjuster_id,
    policy_product,
    policy_holder_id,
    holder_joined_on,
    prev_reported_at,
    holder_joined_on_dense_rank,
    second_holder_joined_on,
    first_holder_joined_on,
    second_reported_at,
    first_known_reported_at,
    prev3_reported_at,
    holder_joined_on_dense_rank_share_of_holder_join,
    holder_joined_on_month_avg_policy_end_date_rank,
    rn,
    holder_joined_on_month_min_holder_joined_on_cume,
    ROUND((UNIX_TIMESTAMP(first_known_reported_at) - UNIX_TIMESTAMP(second_reported_at)) / 3600, 1) AS hours_second_reported_at_to_first_known_reported,
    LENGTH(TRIM(policy_product)) AS policy_product_len
FROM metrics
ORDER BY policy_product_len, prev3_reported_at
