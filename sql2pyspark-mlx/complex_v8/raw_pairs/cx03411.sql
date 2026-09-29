WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CASE WHEN claimed_amount >= 32796.4 THEN 'HIGH' WHEN claimed_amount >= 19337.6 THEN 'MEDIUM' ELSE 'LOW' END AS claimed_amount_tier,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE approved_amount >= 4868.7
      AND status = 'APPROVED'
),
combined_details_adjuster AS (
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
        s.status_len,
        a.level AS adjuster_level
    FROM valid s
    JOIN adjusters a ON a.id = s.adjuster_id
),
policy_id_stats AS (
    -- Median adjuster_level per policy_id
    SELECT policy_id, PERCENTILE_APPROX(adjuster_level, 0.5) AS median_adjuster_level
    FROM combined_details_adjuster
    GROUP BY policy_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_adjuster_level,
        ROUND(s.adjuster_level - g.median_adjuster_level, 2) AS adjuster_level_minus_median
    FROM combined_details_adjuster s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.adjuster_level >= g.median_adjuster_level
),
with_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.adjuster_level,
        s.median_adjuster_level,
        s.adjuster_level_minus_median,
        a.adjuster_name,
        a.team AS adjuster_team,
        p.start_date AS policy_start_date
    FROM outliers s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
enriched_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.median_adjuster_level,
        s.adjuster_level_minus_median,
        s.adjuster_name,
        s.adjuster_team,
        s.policy_start_date,
        a.team AS adjuster_team_2,
        a.adjuster_name AS adjuster_adjuster_name,
        a.level AS adjuster_level_2
    FROM with_details_adjuster s
    JOIN adjusters a ON a.id = s.adjuster_id
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM enriched_adjuster
    WHERE median_adjuster_level > (SELECT AVG(median_adjuster_level) FROM enriched_adjuster)
)
SELECT
    id,
    policy_id,
    claim_type,
    claimed_amount,
    status,
    median_adjuster_level,
    adjuster_level_minus_median,
    adjuster_team,
    adjuster_team_2,
    adjuster_adjuster_name
FROM without_matches
ORDER BY policy_id DESC NULLS FIRST, adjuster_team_2
