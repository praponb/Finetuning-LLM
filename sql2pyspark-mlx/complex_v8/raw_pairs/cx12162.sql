WITH cleaned AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LOWER(TRIM(adjuster_name)) AS adjuster_name_clean
    FROM adjusters
    WHERE UPPER(TRIM(adjuster_name)) LIKE 'F%'
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN team IN ('Health', 'Motor') THEN 'group_a' WHEN team = 'Travel' THEN 'group_b' ELSE 'other' END AS team_group,
        CONCAT(adjuster_name_clean, '#', CAST(level AS STRING)) AS adjuster_name_clean_ref,
        LENGTH(TRIM(adjuster_name_clean)) AS adjuster_name_clean_len
    FROM cleaned
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_name,
        team,
        level,
        adjuster_name_clean,
        team_group,
        adjuster_name_clean_ref,
        adjuster_name_clean_len,
        LAST_VALUE(level) OVER (ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_level,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY level DESC, id) AS level_dense_rank,
        FIRST_VALUE(level) OVER (ORDER BY adjuster_name_clean_len DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_level
    FROM enriched
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(team_group, 'UNKNOWN') AS team_group_or_unknown,
        LEAST(adjuster_name_clean_len, level_dense_rank) AS least_adjuster_name_clean_len_level_dense_rank
    FROM with_trends
)
SELECT
    COALESCE(team_group, 'ALL') AS team_group,
    COALESCE(team, 'ALL') AS team,
    GROUPING(team_group) AS is_total_team_group,
    COUNT(*) AS n_rows
FROM tagged
GROUP BY ROLLUP(team_group, team)
ORDER BY team_group NULLS LAST, team NULLS LAST
