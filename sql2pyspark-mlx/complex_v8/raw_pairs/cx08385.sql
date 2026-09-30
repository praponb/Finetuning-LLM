WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        TO_DATE(reported_at) AS reported_at_date
    FROM claims
    WHERE claim_date < DATE '2025-01-04'
      AND claim_type IN ('medical', 'theft', 'fire')
),
enriched AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.reported_at_date,
        p.product AS policy_product,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM filtered_claims s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    WHERE p.product IN ('TRAVEL', 'HOME', 'AUTO')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_status_by_reported_at_ties, 3) AS reported_at_cume_dist,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY reported_at DESC, id) AS reported_at_dense_rank,
        DENSE_RANK() OVER (ORDER BY approved_amount, id) AS approved_amount_dense_rank,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY adjuster_id, YEAR(reported_at_date) ORDER BY reported_at_date DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS adjuster_level_avg_7,
        NTH_VALUE(adjuster_level, 2) OVER (ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_adjuster_level,
        LEAD(claim_date) OVER w_all_by_adjuster_level AS next_claim_date,
        ROUND(PERCENT_RANK() OVER w_all_by_approved_amount, 3) AS approved_amount_percent_rank,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY adjuster_level), 2) AS approved_amount_vs_adjuster_level_avg,
        SUM(approved_amount) OVER (ORDER BY approved_amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS approved_amount_sum_2_prior,
        LAG(adjuster_level, 2) OVER (PARTITION BY adjuster_id, YEAR(reported_at_date) ORDER BY reported_at_date DESC, id) AS prev2_adjuster_level,
        SUM(approved_amount) OVER w_adjuster_level AS adjuster_level_sum_approved_amount,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_policy_id, 0), 4) AS claimed_amount_share_of_policy_id
    FROM enriched
    WINDOW w_status_by_reported_at_ties AS (PARTITION BY status ORDER BY reported_at DESC), w_all_by_adjuster_level AS (ORDER BY adjuster_level DESC, id), w_all_by_approved_amount AS (ORDER BY approved_amount, id), w_adjuster_level AS (PARTITION BY adjuster_level), w_policy_id AS (PARTITION BY policy_id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM with_windows
    WHERE reported_at_dense_rank <= 5
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        status,
        reported_at,
        reported_at_date,
        policy_product,
        adjuster_name,
        adjuster_level,
        rows_in_policy_id,
        reported_at_dense_rank,
        approved_amount_dense_rank,
        adjuster_level_avg_7,
        second_adjuster_level,
        approved_amount_percent_rank,
        approved_amount_sum_2_prior,
        prev2_adjuster_level,
        claimed_amount_share_of_policy_id,
        ROW_NUMBER() OVER (PARTITION BY policy_product, prev2_adjuster_level ORDER BY approved_amount_sum_2_prior, id) AS rn,
        NTILE(5) OVER w_policy_product_prev2_adjuste AS approved_amount_sum_2_prior_quintile,
        second_adjuster_level - LAG(second_adjuster_level) OVER (PARTITION BY policy_product, prev2_adjuster_level ORDER BY approved_amount_sum_2_prior, id) AS second_adjuster_level_change,
        FIRST_VALUE(claim_date) OVER (PARTITION BY status, reported_at_dense_rank ORDER BY approved_amount_sum_2_prior ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date
    FROM top_per_group
    WINDOW w_policy_product_prev2_adjuste AS (PARTITION BY policy_product, prev2_adjuster_level ORDER BY approved_amount_sum_2_prior, id)
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    status,
    reported_at,
    reported_at_date,
    policy_product,
    adjuster_name,
    adjuster_level,
    rows_in_policy_id,
    reported_at_dense_rank,
    approved_amount_dense_rank,
    second_adjuster_level,
    approved_amount_percent_rank,
    prev2_adjuster_level,
    claimed_amount_share_of_policy_id,
    rn,
    approved_amount_sum_2_prior_quintile,
    second_adjuster_level_change,
    first_claim_date,
    COALESCE(status, 'UNKNOWN') AS status_or_unknown,
    LENGTH(TRIM(policy_product)) AS policy_product_len
FROM ranked
ORDER BY status_or_unknown, status DESC NULLS FIRST
