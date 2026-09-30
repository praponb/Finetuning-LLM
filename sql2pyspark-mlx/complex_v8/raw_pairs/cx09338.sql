WITH scoped AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(adjuster_name), ' ', '_')) AS adjuster_name_slug,
        LENGTH(TRIM(team)) AS team_len
    FROM adjusters
    WHERE level > 3
      AND team IN ('Health', 'Motor', 'Property')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        team,
        level,
        team_len,
        ROW_NUMBER() OVER (PARTITION BY level, team_len ORDER BY team_len NULLS LAST, id) AS rn,
        LAST_VALUE(team_len) IGNORE NULLS OVER (ORDER BY team_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_team_len,
        COUNT(*) OVER (PARTITION BY team, team_or_unknown) AS rows_in_team_team_or_unknown,
        SUM(team_len) OVER (PARTITION BY team_or_unknown ORDER BY level NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS team_len_sum_6_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY team_or_unknown ORDER BY level NULLS LAST, id), 3) AS level_cume_dist
    FROM scoped
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN rn >= 8 THEN 'large' WHEN rn >= 3 THEN 'medium' ELSE 'small' END AS rn_tier,
        LOWER(TRIM(team)) AS team_clean
    FROM with_windows
)
SELECT id, metric, amount
FROM (
    SELECT id, rn, rows_in_team_team_or_unknown
    FROM labeled
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric IN (rn AS rn_v, rows_in_team_team_or_unknown AS m_rows_in_team_team_or_unknown)
) unp
ORDER BY id, metric
