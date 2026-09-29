WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        reported_at
    FROM claims
    WHERE claim_date < DATE '2025-04-25'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY policy_id ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        SUM(approved_amount) OVER (PARTITION BY policy_id ORDER BY reported_at ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS approved_amount_sum_3_prior,
        LAST_VALUE(reported_at) OVER (PARTITION BY claim_type ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at,
        ROUND(CUME_DIST() OVER (ORDER BY approved_amount, id), 3) AS approved_amount_cume_dist,
        MIN(approved_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_approved_amount_so_far,
        ROUND(CUME_DIST() OVER (ORDER BY reported_at), 3) AS reported_at_cume_dist,
        SUM(approved_amount) OVER (ORDER BY approved_amount, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS approved_amount_sum_1_prior,
        MIN(approved_amount) OVER (PARTITION BY policy_id, adjuster_id) AS policy_id_adjuster_id_min_approved_amount,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY policy_id, adjuster_id)) AS distinct_claim_type_in_policy_id_adjuster_id,
        LAST_VALUE(reported_at) OVER (ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at_2,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id, adjuster_id), 0), 4) AS approved_amount_share_of_policy_id_adjuster_id
    FROM base_claims
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(second_approved_amount / NULLIF(approved_amount, 0), 3) AS second_approved_amount_per_approved_amount,
        DATE_TRUNC('YEAR', last_reported_at) AS last_reported_at_year
    FROM with_trends
    WHERE policy_id_adjuster_id_min_approved_amount BETWEEN 125000.0 AND 450000.0
)
SELECT
    id,
    policy_id,
    claim_type,
    approved_amount,
    reported_at,
    approved_amount_sum_3_prior,
    last_reported_at,
    approved_amount_cume_dist,
    min_approved_amount_so_far,
    approved_amount_sum_1_prior,
    policy_id_adjuster_id_min_approved_amount,
    distinct_claim_type_in_policy_id_adjuster_id,
    second_approved_amount_per_approved_amount,
    DATEDIFF(DATE '2025-12-31', last_reported_at_2) AS days_since_last_reported_at_2,
    YEAR(reported_at) AS reported_at_year
FROM enriched
ORDER BY approved_amount_cume_dist DESC
