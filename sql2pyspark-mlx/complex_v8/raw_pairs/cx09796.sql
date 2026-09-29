WITH valid AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        UPPER(TRIM(adjuster_name)) AS adjuster_name_clean,
        LENGTH(TRIM(team)) AS team_len
    FROM adjusters
    WHERE adjuster_name IS NOT NULL
      AND team IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(level, 2) OVER (PARTITION BY team ORDER BY level NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_level,
        COUNT(*) OVER w_team AS rows_in_team,
        RANK() OVER (PARTITION BY team ORDER BY level NULLS LAST) AS level_rank,
        MIN(level) OVER (PARTITION BY team ORDER BY level NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_level_so_far
    FROM valid
    WINDOW w_team AS (PARTITION BY team)
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, COUNT(*) AS claim_count
    FROM claims
    GROUP BY adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM sequenced s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        min_level_so_far + rows_in_team AS min_level_so_far_plus_rows_in_team,
        CONCAT(adjuster_name, '#', CAST(rows_in_team AS STRING)) AS adjuster_name_ref
    FROM summarized_rows
),
team_stats AS (
    -- Median second_level per team
    SELECT team, PERCENTILE_APPROX(second_level, 0.5) AS median_second_level
    FROM labeled
    GROUP BY team
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_second_level,
        ROUND(s.second_level - g.median_second_level, 2) AS second_level_minus_median
    FROM labeled s
    JOIN team_stats g ON g.team = s.team
    WHERE s.second_level >= g.median_second_level
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_name,
        team,
        level,
        adjuster_name_clean,
        team_len,
        second_level,
        min_level_so_far,
        claim_count,
        adjuster_name_ref,
        median_second_level,
        second_level_minus_median,
        ROUND(CUME_DIST() OVER (PARTITION BY team_len, claim_count ORDER BY level_rank DESC), 3) AS level_rank_cume_dist,
        MAX(team_len) OVER (PARTITION BY level) AS level_max_team_len,
        LAG(rows_in_team, 1, 0) OVER (PARTITION BY team_len, claim_count ORDER BY level_rank DESC, id) AS prev_rows_in_team,
        ROW_NUMBER() OVER w_team_len_claim_count_by_leve_2 AS rn,
        MAX(second_level) OVER (PARTITION BY level) AS level_max_second_level
    FROM vs_group
    WINDOW w_team_len_claim_count_by_leve_2 AS (PARTITION BY team_len, claim_count ORDER BY level_rank DESC, id)
)
SELECT
    id,
    adjuster_name,
    team,
    level,
    adjuster_name_clean,
    second_level,
    min_level_so_far,
    claim_count,
    adjuster_name_ref,
    median_second_level,
    second_level_minus_median,
    level_rank_cume_dist,
    level_max_team_len,
    prev_rows_in_team,
    level_max_second_level,
    CONCAT(adjuster_name_ref, '#', CAST(prev_rows_in_team AS STRING)) AS adjuster_name_ref_ref
FROM with_trends
ORDER BY median_second_level DESC, prev_rows_in_team
