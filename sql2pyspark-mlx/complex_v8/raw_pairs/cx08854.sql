WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status
    FROM claims
    WHERE claim_type IS NOT NULL
),
with_parent_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        p.start_date AS policy_start_date,
        po.joined_on AS holder_joined_on
    FROM recent_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY adjuster_id, policy_id) AS rows_in_adjuster_id_policy_id,
        DENSE_RANK() OVER (PARTITION BY adjuster_id ORDER BY policy_start_date DESC, id) AS policy_start_date_dense_rank,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY adjuster_id, policy_id), 2) AS approved_amount_vs_adjuster_id_policy_id_avg,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY approved_amount NULLS LAST) AS approved_amount_dense_rank,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY policy_id, status), 2) AS approved_amount_vs_policy_id_status_avg,
        ROW_NUMBER() OVER w_all_by_approved_amount AS seq_no,
        FIRST_VALUE(holder_joined_on) OVER (PARTITION BY adjuster_id ORDER BY policy_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_holder_joined_on
    FROM with_parent_policy
    WINDOW w_all_by_approved_amount AS (ORDER BY approved_amount, id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        policy_start_date,
        holder_joined_on,
        rows_in_adjuster_id_policy_id,
        policy_start_date_dense_rank,
        approved_amount_vs_adjuster_id_policy_id_avg,
        approved_amount_dense_rank,
        approved_amount_vs_policy_id_status_avg,
        first_holder_joined_on
    FROM with_trends
    WHERE seq_no <= 3
)
SELECT
    id,
    policy_id,
    approved_amount,
    status,
    rows_in_adjuster_id_policy_id,
    policy_start_date_dense_rank,
    approved_amount_vs_adjuster_id_policy_id_avg,
    approved_amount_vs_policy_id_status_avg,
    first_holder_joined_on,
    ABS(approved_amount - 20843.1) AS approved_amount_distance,
    COALESCE(approved_amount, 0.0) AS approved_amount_filled
FROM top_per_group
ORDER BY approved_amount_vs_adjuster_id_policy_id_avg NULLS LAST, policy_id NULLS LAST
