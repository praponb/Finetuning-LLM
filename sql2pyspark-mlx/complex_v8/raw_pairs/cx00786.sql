WITH prepared AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        UPPER(TRIM(adjuster_name)) AS adjuster_name_clean,
        COALESCE(level, 0) AS level_filled,
        LOWER(REGEXP_REPLACE(TRIM(adjuster_name), ' ', '_')) AS adjuster_name_slug
    FROM adjusters
    WHERE level >= 2
      AND team IS NOT NULL
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        level_filled,
        team,
        level,
        SUM(CASE WHEN team = 'Travel' THEN 1 ELSE 0 END) AS n_team_travel,
        SUM(level_filled) AS total_level_filled,
        ARRAY_SORT(COLLECT_SET(team)) AS team_list,
        ROUND(STDDEV(level_filled), 2) AS stddev_level_filled
    FROM prepared
    GROUP BY level_filled, team, level
),
enriched AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(team), 1, 4) AS team_prefix
    FROM rollup_base
),
per_label AS (
    -- One row per team_list_item
    SELECT
        s.level_filled,
        s.team,
        s.level,
        s.n_team_travel,
        s.total_level_filled,
        s.stddev_level_filled,
        s.team_prefix,
        t.team_list_item
    FROM enriched s
    LATERAL VIEW EXPLODE(s.team_list) t AS team_list_item
)
SELECT
    level_filled,
    team,
    level,
    n_team_travel,
    total_level_filled,
    stddev_level_filled,
    team_list_item,
    stddev_level_filled - level_filled AS stddev_level_filled_minus_level_filled,
    LENGTH(TRIM(team)) AS team_len
FROM per_label
ORDER BY n_team_travel NULLS LAST
