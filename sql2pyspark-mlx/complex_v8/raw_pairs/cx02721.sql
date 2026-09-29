WITH cleaned_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE status IS NOT NULL
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.team AS adjuster_team,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM cleaned_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(adjuster_level) OVER (PARTITION BY TO_DATE(reported_at) ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_adjuster_level,
        MAX(claimed_amount) OVER (PARTITION BY TO_DATE(reported_at) ORDER BY claimed_amount DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS claimed_amount_max_7
    FROM combined_details
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        adjuster_team,
        adjuster_level,
        ROUND(CUME_DIST() OVER (ORDER BY adjuster_level, id), 3) AS adjuster_level_cume_dist,
        LEAD(reported_at, 3) OVER (ORDER BY adjuster_level, id) AS next3_reported_at,
        SIZE(COLLECT_SET(claim_type) OVER w_adjuster_team_first_adjuster) AS distinct_claim_type_in_adjuster_team_first_adjus,
        ROW_NUMBER() OVER (PARTITION BY adjuster_level ORDER BY claim_date DESC, id) AS seq_no,
        NTILE(3) OVER (ORDER BY adjuster_level, id) AS adjuster_level_tercile
    FROM with_trends
    WINDOW w_adjuster_team_first_adjuster AS (PARTITION BY adjuster_team, first_adjuster_level)
)
-- Subtotals with CUBE
SELECT
    claim_type,
    distinct_claim_type_in_adjuster_team_first_adjus,
    COUNT(*) AS row_count,
    SUM(CASE WHEN claim_type = 'fire' THEN 1 ELSE 0 END) AS n_claim_type_fire
FROM metrics
GROUP BY CUBE(claim_type, distinct_claim_type_in_adjuster_team_first_adjus)
ORDER BY claim_type NULLS LAST, distinct_claim_type_in_adjuster_team_first_adjus NULLS LAST
