WITH valid AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        UPPER(TRIM(adjuster_name)) AS adjuster_name_clean,
        CASE WHEN level >= 3 THEN 'A' WHEN level >= 2 THEN 'B' ELSE 'C' END AS level_tier,
        CONCAT_WS('-', team, adjuster_name) AS team_adjuster_name_label
    FROM adjusters
    WHERE LENGTH(TRIM(adjuster_name)) >= 4
      AND level <= 3
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        adjuster_name,
        level_tier,
        LENGTH(TRIM(adjuster_name_clean)) AS adjuster_name_clean_len,
        LENGTH(TRIM(team_adjuster_name_label)) AS team_adjuster_name_label_len
    FROM valid
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        INITCAP(TRIM(adjuster_name)) AS adjuster_name_clean,
        LOWER(REGEXP_REPLACE(TRIM(level_tier), ' ', '_')) AS level_tier_slug
    FROM scored
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, adjuster_name_clean_len, team_adjuster_name_label_len
    FROM derived
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (adjuster_name_clean_len AS m_adjuster_name_clean_len, team_adjuster_name_label_len AS team_adjuster_name_label_len_v)
) unp
ORDER BY id, field
