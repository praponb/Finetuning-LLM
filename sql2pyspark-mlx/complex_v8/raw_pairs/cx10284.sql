WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE policy_id IS NOT NULL
),
with_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.status,
        s.reported_at,
        a.level AS adjuster_level,
        p.premium AS policy_premium
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(policy_premium) OVER (PARTITION BY status ORDER BY adjuster_level NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS policy_premium_avg_7,
        FIRST_VALUE(adjuster_level) IGNORE NULLS OVER (PARTITION BY adjuster_level ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_adjuster_level,
        adjuster_level - LAG(adjuster_level) OVER w_reported_at_to_date_status_b AS adjuster_level_change,
        MIN(policy_premium) OVER (PARTITION BY claim_type, status) AS claim_type_status_min_policy_premium,
        SIZE(COLLECT_SET(policy_id) OVER w_claim_type_status) AS distinct_policy_id_in_claim_type_status,
        SUM(policy_premium) OVER (PARTITION BY claim_type, status) AS claim_type_status_sum_policy_premium,
        NTILE(5) OVER (PARTITION BY status ORDER BY adjuster_level NULLS LAST, id) AS adjuster_level_quintile,
        LAST_VALUE(adjuster_level) OVER (PARTITION BY adjuster_level ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_adjuster_level,
        MAX(adjuster_level) OVER (PARTITION BY adjuster_level ORDER BY reported_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS adjuster_level_max_3,
        ROUND(CUME_DIST() OVER w_status_by_adjuster_level, 3) AS adjuster_level_cume_dist,
        LEAD(reported_at, 2) OVER w_reported_at_to_date_status_b AS next2_reported_at,
        COUNT(*) OVER (PARTITION BY claim_type, status) AS rows_in_claim_type_status,
        LAST_VALUE(adjuster_level) IGNORE NULLS OVER (PARTITION BY status ORDER BY adjuster_level NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_adjuster_level,
        LEAD(policy_premium) OVER w_reported_at_to_date_status_b AS next_policy_premium
    FROM with_details
    WINDOW w_reported_at_to_date_status_b AS (PARTITION BY TO_DATE(reported_at), status ORDER BY adjuster_level DESC, id), w_claim_type_status AS (PARTITION BY claim_type, status), w_status_by_adjuster_level AS (PARTITION BY status ORDER BY adjuster_level NULLS LAST, id)
),
enriched AS (
    -- Derive helper columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM with_windows
    WHERE policy_premium_avg_7 < 4520.0
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_claim_type_adjuster_level_ch AS rows_in_claim_type_adjuster_level_change,
        ROUND(AVG(policy_premium) OVER (PARTITION BY claim_type, adjuster_id ORDER BY distinct_policy_id_in_claim_type_status DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS policy_premium_avg_1_prior,
        FIRST_VALUE(first_known_adjuster_level) IGNORE NULLS OVER (PARTITION BY distinct_policy_id_in_claim_type_status ORDER BY last_known_adjuster_level, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_adjuster_level,
        next_policy_premium - LAG(next_policy_premium) OVER (PARTITION BY policy_id, adjuster_level_quintile ORDER BY adjuster_level_cume_dist DESC, id) AS next_policy_premium_change
    FROM enriched
    WINDOW w_claim_type_adjuster_level_ch AS (PARTITION BY claim_type, adjuster_level_change)
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_type,
    adjuster_level,
    policy_premium,
    policy_premium_avg_7,
    first_known_adjuster_level,
    claim_type_status_min_policy_premium,
    distinct_policy_id_in_claim_type_status,
    adjuster_level_quintile,
    adjuster_level_max_3,
    adjuster_level_cume_dist,
    next2_reported_at,
    last_known_adjuster_level,
    next_policy_premium,
    status_slug,
    policy_premium_avg_1_prior,
    first_known_first_known_adjuster_level,
    next_policy_premium_change,
    COALESCE(policy_premium, 0.0) AS policy_premium_filled
FROM with_trends
WHERE adjuster_id >= 3
ORDER BY adjuster_level_quintile
