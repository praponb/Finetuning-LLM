WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM claims
    WHERE approved_amount BETWEEN 0.0 AND 23487.5
),
with_details AS (
    -- Join policies
    SELECT
        s.*,
        p.holder_id AS policy_holder_id,
        p.end_date AS policy_end_date,
        po.segment AS holder_segment
    FROM base s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(holder_segment) OVER w_adjuster_id_status_or_unknow) AS distinct_holder_segment_in_adjuster_id_status_or,
        NTILE(5) OVER w_all_by_reported_at AS reported_at_quintile,
        COUNT(*) OVER (PARTITION BY adjuster_id, status_or_unknown) AS rows_in_adjuster_id_status_or_unknown,
        MIN(approved_amount) OVER (PARTITION BY status_or_unknown, holder_segment) AS status_or_unknown_holder_segment_min_approved_am,
        COUNT(*) OVER (PARTITION BY status_or_unknown, holder_segment) AS rows_in_status_or_unknown_holder_segment,
        NTH_VALUE(policy_end_date, 2) OVER (ORDER BY reported_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_policy_end_date,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY adjuster_id, status_or_unknown), 0), 4) AS claimed_amount_share_of_adjuster_id_status_or_un,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY status_or_unknown, claim_type), 0), 4) AS claimed_amount_share_of_status_or_unknown_claim_,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id, status_or_unknown)) AS distinct_policy_id_in_adjuster_id_status_or_unkn,
        LAST_VALUE(claimed_amount) OVER (ORDER BY reported_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        COUNT(*) OVER w_status_or_unknown_claim_type AS rows_in_status_or_unknown_claim_type,
        DENSE_RANK() OVER (PARTITION BY status_or_unknown ORDER BY approved_amount ASC) AS approved_amount_dense_rank,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER w_claim_type_by_reported_at)) / 60, 1) AS minutes_since_prev_reported_at
    FROM with_details
    WINDOW w_adjuster_id_status_or_unknow AS (PARTITION BY adjuster_id, status_or_unknown), w_all_by_reported_at AS (ORDER BY reported_at NULLS LAST, id), w_status_or_unknown_claim_type AS (PARTITION BY status_or_unknown, claim_type), w_claim_type_by_reported_at AS (PARTITION BY claim_type ORDER BY reported_at DESC, id)
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        status_or_unknown,
        policy_holder_id,
        policy_end_date,
        holder_segment,
        distinct_holder_segment_in_adjuster_id_status_or,
        reported_at_quintile,
        rows_in_adjuster_id_status_or_unknown,
        status_or_unknown_holder_segment_min_approved_am,
        rows_in_status_or_unknown_holder_segment,
        second_policy_end_date,
        claimed_amount_share_of_adjuster_id_status_or_un,
        claimed_amount_share_of_status_or_unknown_claim_,
        distinct_policy_id_in_adjuster_id_status_or_unkn,
        last_claimed_amount,
        rows_in_status_or_unknown_claim_type,
        minutes_since_prev_reported_at
    FROM ranked
    WHERE approved_amount_dense_rank <= 2
      AND YEAR(claim_date) = 2025
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    approved_amount,
    reported_at,
    status_or_unknown,
    policy_end_date,
    distinct_holder_segment_in_adjuster_id_status_or,
    reported_at_quintile,
    rows_in_adjuster_id_status_or_unknown,
    status_or_unknown_holder_segment_min_approved_am,
    rows_in_status_or_unknown_holder_segment,
    second_policy_end_date,
    claimed_amount_share_of_adjuster_id_status_or_un,
    distinct_policy_id_in_adjuster_id_status_or_unkn,
    last_claimed_amount,
    rows_in_status_or_unknown_claim_type,
    minutes_since_prev_reported_at
FROM top_per_group
ORDER BY approved_amount, status_or_unknown_holder_segment_min_approved_am DESC NULLS FIRST
