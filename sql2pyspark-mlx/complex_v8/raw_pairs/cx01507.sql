WITH filtered AS (
    -- Filter claims
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
        CASE WHEN approved_amount >= 22250.6 THEN 'A' WHEN approved_amount >= 0.0 THEN 'B' ELSE 'C' END AS approved_amount_tier,
        reported_at + INTERVAL 15 MINUTES AS reported_at_plus_15m
    FROM claims
    WHERE policy_id >= 5
),
with_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.*,
        a.adjuster_name,
        a.level AS adjuster_level,
        p.premium AS policy_premium,
        p.status AS policy_status
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
    WHERE LOWER(a.adjuster_name) LIKE '%ca%'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at,
        approved_amount_tier,
        adjuster_name,
        adjuster_level,
        NTH_VALUE(reported_at_plus_15m, 2) OVER (PARTITION BY MONTH(reported_at_plus_15m) ORDER BY reported_at_plus_15m ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at_plus_15m,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(reported_at_plus_15m) ORDER BY reported_at_plus_15m ASC), 3) AS reported_at_plus_15m_percent_rank,
        SUM(claimed_amount) OVER (PARTITION BY policy_status) AS policy_status_sum_claimed_amount,
        DENSE_RANK() OVER (PARTITION BY policy_status ORDER BY adjuster_level NULLS LAST) AS adjuster_level_dense_rank,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY policy_status)) AS distinct_claim_type_in_policy_status,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY policy_status), 2) AS adjuster_level_vs_policy_status_avg,
        COUNT(*) OVER w_policy_status AS rows_in_policy_status,
        LAST_VALUE(policy_premium) IGNORE NULLS OVER (PARTITION BY MONTH(reported_at_plus_15m) ORDER BY reported_at_plus_15m ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_policy_premium,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER (PARTITION BY policy_status), 0), 4) AS adjuster_level_share_of_policy_status,
        LAST_VALUE(reported_at_plus_15m) IGNORE NULLS OVER (ORDER BY reported_at_plus_15m ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at_plus_15m
    FROM with_details_adjuster
    WINDOW w_policy_status AS (PARTITION BY policy_status)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        adjuster_level + rows_in_policy_status AS adjuster_level_plus_rows_in_policy_status
    FROM metrics
    WHERE policy_status_sum_claimed_amount <= 325035.0
)
SELECT
    id,
    adjuster_id,
    claimed_amount,
    status,
    reported_at,
    adjuster_name,
    adjuster_level,
    second_reported_at_plus_15m,
    reported_at_plus_15m_percent_rank,
    distinct_claim_type_in_policy_status,
    rows_in_policy_status,
    last_known_policy_premium,
    adjuster_level_share_of_policy_status,
    last_known_reported_at_plus_15m,
    adjuster_level_plus_rows_in_policy_status,
    TO_DATE(reported_at) AS reported_at_date,
    LEAST(claimed_amount, rows_in_policy_status) AS least_claimed_amount_rows_in_policy_status
FROM derived
ORDER BY distinct_claim_type_in_policy_status DESC, reported_at
