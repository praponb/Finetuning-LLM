WITH cleaned AS (
    -- Keep valid rows and derive helper columns
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
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        DATE_ADD(claim_date, 30) AS claim_date_plus_30d
    FROM claims
),
joined AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claimed_amount_filled,
        s.claim_date_plus_30d,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        p.premium AS policy_premium
    FROM cleaned s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        claim_type,
        claimed_amount,
        approved_amount,
        claimed_amount_filled,
        claim_date_plus_30d,
        adjuster_level,
        policy_premium,
        YEAR(claim_date_plus_30d) AS claim_date_plus_30d_year
    FROM joined
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        claimed_amount,
        approved_amount,
        claimed_amount_filled,
        adjuster_level,
        claim_date_plus_30d_year,
        LAG(adjuster_level) OVER (PARTITION BY claim_date_plus_30d_year, policy_id ORDER BY policy_premium, id) AS prev_adjuster_level,
        SIZE(COLLECT_SET(claim_type) OVER w_policy_id) AS distinct_claim_type_in_policy_id,
        SUM(claimed_amount_filled) OVER (PARTITION BY policy_id) AS policy_id_sum_claimed_amount_filled,
        approved_amount - LAG(approved_amount) OVER (PARTITION BY adjuster_level ORDER BY adjuster_level DESC, id) AS approved_amount_change,
        DENSE_RANK() OVER (PARTITION BY adjuster_level ORDER BY adjuster_level DESC, id) AS adjuster_level_dense_rank
    FROM derived
    WINDOW w_policy_id AS (PARTITION BY policy_id)
)
-- Subtotals with GROUPING SETS
SELECT
    prev_adjuster_level,
    distinct_claim_type_in_policy_id,
    GROUPING_ID() AS grouping_level,
    PERCENTILE_APPROX(policy_id_sum_claimed_amount_filled, 0.5) AS median_policy_id_sum_claimed_amount_filled,
    COUNT(*) AS cnt
FROM with_windows
GROUP BY GROUPING SETS ((prev_adjuster_level, distinct_claim_type_in_policy_id), (prev_adjuster_level), (distinct_claim_type_in_policy_id), ())
ORDER BY prev_adjuster_level NULLS LAST, distinct_claim_type_in_policy_id NULLS LAST
