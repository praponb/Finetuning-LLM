WITH scoped_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CASE WHEN claimed_amount >= 31715.7 THEN 'large' WHEN claimed_amount >= 19609.8 THEN 'medium' ELSE 'small' END AS claimed_amount_tier,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM claims
    WHERE (claim_type <> 'collision' OR claimed_amount <= 41878.5)
),
with_parent AS (
    -- Attach policy attributes
    SELECT
        s.*,
        p.end_date AS policy_end_date,
        p.status AS policy_status,
        p.start_date AS policy_start_date
    FROM scoped_claims s
    JOIN policies p ON p.id = s.policy_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER w_policy_start_date_month_by_c AS claim_date_quintile,
        LAST_VALUE(policy_start_date) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', policy_start_date) ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_policy_start_date,
        SIZE(COLLECT_SET(claimed_amount_tier) OVER (PARTITION BY YEAR(policy_end_date), YEAR(policy_start_date))) AS distinct_claimed_amount_tier_in_policy_end_date_,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', policy_start_date) ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        SUM(claimed_amount) OVER (PARTITION BY adjuster_id, claimed_amount_tier ORDER BY approved_amount DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS claimed_amount_sum_3,
        NTILE(5) OVER (PARTITION BY adjuster_id, claimed_amount_tier ORDER BY approved_amount DESC, id) AS approved_amount_quintile,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', policy_start_date) ORDER BY claim_date DESC, id), 3) AS claim_date_cume_dist,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY YEAR(policy_end_date), YEAR(policy_start_date)), 2) AS policy_end_date_year_policy_start_date_year_avg_,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', policy_start_date) ORDER BY claim_date DESC), 3) AS claim_date_cume_dist_2,
        SUM(claimed_amount_bucket) OVER (PARTITION BY YEAR(policy_end_date), YEAR(policy_start_date)) AS policy_end_date_year_policy_start_date_year_sum_,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', policy_start_date) ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        ROW_NUMBER() OVER (PARTITION BY claimed_amount_tier, policy_status ORDER BY policy_start_date NULLS LAST, id) AS seq_no,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY YEAR(policy_end_date), YEAR(policy_start_date))) AS distinct_adjuster_id_in_policy_end_date_year_pol
    FROM with_parent
    WINDOW w_policy_start_date_month_by_c AS (PARTITION BY DATE_TRUNC('MONTH', policy_start_date) ORDER BY claim_date DESC, id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE seq_no = 1
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(claimed_amount) OVER w_status, 2) AS status_avg_claimed_amount,
        LAST_VALUE(seq_no) OVER (PARTITION BY adjuster_id, policy_status ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_seq_no,
        ROW_NUMBER() OVER (PARTITION BY distinct_adjuster_id_in_policy_end_date_year_pol ORDER BY seq_no DESC, id) AS seq_no_2
    FROM top_per_group
    WINDOW w_status AS (PARTITION BY status)
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claimed_amount,
    approved_amount,
    status,
    reported_at,
    claimed_amount_tier,
    claimed_amount_bucket,
    policy_end_date,
    policy_status,
    policy_start_date,
    claimed_amount_sum_3,
    approved_amount_quintile,
    claim_date_cume_dist,
    claim_date_cume_dist_2,
    policy_end_date_year_policy_start_date_year_sum_,
    last_claimed_amount,
    seq_no,
    status_avg_claimed_amount,
    seq_no_2,
    CONCAT(claimed_amount_tier, '#', CAST(adjuster_id AS STRING)) AS claimed_amount_tier_ref,
    ABS(claimed_amount_bucket - 15070) AS claimed_amount_bucket_distance
FROM with_windows_2
ORDER BY adjuster_id DESC, claimed_amount NULLS LAST, id
LIMIT 5
