WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        reported_at,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM claims
    WHERE status IS NOT NULL
      AND claim_type = 'medical'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY TO_DATE(reported_at), adjuster_id), 2) AS claimed_amount_vs_reported_at_to_date_adjuster_i,
        FIRST_VALUE(reported_at) OVER (PARTITION BY claim_type, adjuster_id ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at,
        NTH_VALUE(reported_at, 2) OVER (ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        COUNT(*) OVER (PARTITION BY TO_DATE(reported_at), adjuster_id) AS rows_in_reported_at_to_date_adjuster_id,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY claim_type), 2) AS claimed_amount_vs_claim_type_avg,
        DENSE_RANK() OVER (ORDER BY reported_at DESC NULLS FIRST, id) AS reported_at_dense_rank,
        SUM(claimed_amount) OVER (PARTITION BY claim_type, policy_id ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id)) AS distinct_policy_id_in_adjuster_id
    FROM cleaned
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        reported_at,
        claim_type_slug,
        claimed_amount_vs_reported_at_to_date_adjuster_i,
        first_reported_at,
        second_reported_at,
        rows_in_reported_at_to_date_adjuster_id,
        claimed_amount_vs_claim_type_avg,
        running_claimed_amount,
        distinct_policy_id_in_adjuster_id
    FROM with_windows
    WHERE reported_at_dense_rank <= 3
      AND claimed_amount_vs_reported_at_to_date_adjuster_i >= -15000.0
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claimed_amount,
    reported_at,
    claim_type_slug,
    claimed_amount_vs_reported_at_to_date_adjuster_i,
    first_reported_at,
    second_reported_at,
    rows_in_reported_at_to_date_adjuster_id,
    claimed_amount_vs_claim_type_avg,
    running_claimed_amount
FROM first_rows
ORDER BY claim_type DESC
