WITH recent_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LENGTH(TRIM(adjuster_name)) AS adjuster_name_len,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug_2
    FROM adjusters
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        adjuster_name_len,
        team_slug,
        ABS(adjuster_name_len - 7) AS adjuster_name_len_distance,
        GREATEST(level, adjuster_name_len) AS greatest_level_adjuster_name_len,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown
    FROM recent_adjusters
),
latest_per_key AS (
    -- Keep the latest row per team_or_unknown
    SELECT
        id,
        adjuster_name,
        team,
        level,
        adjuster_name_len,
        team_slug,
        adjuster_name_len_distance,
        greatest_level_adjuster_name_len,
        team_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team_or_unknown ORDER BY greatest_level_adjuster_name_len DESC, id DESC) AS recency_rank
        FROM scored
    ) ranked
    WHERE recency_rank = 1
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(team_or_unknown), ' ', '_')) AS team_or_unknown_slug,
        LENGTH(TRIM(team)) AS team_len
    FROM latest_per_key
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT team_len, team, adjuster_name_len
    FROM derived
) src
PIVOT (
    MAX(adjuster_name_len)
    FOR team IN ('Travel', 'Property')
)
ORDER BY team_len
