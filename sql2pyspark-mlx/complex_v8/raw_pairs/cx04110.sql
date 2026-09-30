WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LENGTH(TRIM(team)) AS team_len,
        LENGTH(TRIM(team)) AS team_len_2,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown
    FROM adjusters
    WHERE level >= 2
      AND adjuster_name IS NOT NULL
),
annotated AS (
    -- Derive helper columns
    SELECT *, ABS(level - 3) AS level_distance
    FROM base
    WHERE team_len_2 BETWEEN 6 AND 10
),
team_or_unknown_stats AS (
    -- Median team_len_2 per team_or_unknown
    SELECT team_or_unknown, PERCENTILE_APPROX(team_len_2, 0.5) AS median_team_len_2
    FROM annotated
    GROUP BY team_or_unknown
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_team_len_2,
        ROUND(s.team_len_2 - g.median_team_len_2, 2) AS team_len_2_minus_median
    FROM annotated s
    JOIN team_or_unknown_stats g ON g.team_or_unknown = s.team_or_unknown
    WHERE s.team_len_2 >= g.median_team_len_2
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        adjuster_name,
        level,
        team_len_2,
        team_or_unknown,
        team_len_2_minus_median,
        COALESCE(team_or_unknown, 'UNKNOWN') AS team_or_unknown_or_unknown
    FROM compared
    WHERE adjuster_name IS NOT NULL
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.id) AS claim_count,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.adjuster_id = s.id) AS last_claim_reported_at,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.id) AS claim_count_2
    FROM scored s
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN level >= 3 THEN 'large' WHEN level >= 1 THEN 'medium' ELSE 'small' END AS level_tier,
        LENGTH(TRIM(team_or_unknown)) AS team_or_unknown_len
    FROM profiled
),
claim_stats_2 AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MAX(reported_at) AS last_claim_reported_at_2,
        MAX(claimed_amount) AS max_claim_claimed_amount,
        COUNT(*) FILTER (WHERE claim_type IN ('delay', 'fire')) AS claim_claim_type_count
    FROM claims
    GROUP BY adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_reported_at_2,
        c.max_claim_claimed_amount,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count
    FROM derived s
    LEFT JOIN claim_stats_2 c ON c.adjuster_id = s.id
)
SELECT id, metric_name, amount
FROM (
    SELECT id, team_len_2, level, claim_count, claim_claim_type_count
    FROM with_stats
) src
UNPIVOT (
    amount FOR metric_name IN (team_len_2, level, claim_count, claim_claim_type_count)
) unp
