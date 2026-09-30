WITH scoped_adjusters AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CONCAT(team, '#', CAST(level AS STRING)) AS team_ref,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug,
        LOWER(TRIM(adjuster_name)) AS adjuster_name_clean
    FROM adjusters
    WHERE UPPER(TRIM(adjuster_name)) LIKE 'E%'
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM scoped_adjusters s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
              AND c.claimed_amount > 17759.3
              AND c.approved_amount > 0.0
        )
      AND LENGTH(TRIM(s.adjuster_name_clean)) >= 8
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_name,
        team,
        level,
        team_slug,
        ROUND(AVG(level) OVER (PARTITION BY team, level), 2) AS team_level_avg_level,
        ROUND(AVG(level) OVER (ORDER BY level, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS level_avg_4,
        LEAD(level, 3) OVER (ORDER BY level, id) AS next3_level,
        MIN(level) OVER (PARTITION BY team, level) AS team_level_min_level,
        ROUND(AVG(level) OVER (ORDER BY level, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS level_avg_2_prior
    FROM filtered
),
scored AS (
    -- Add calculated columns
    SELECT *, COALESCE(team, 'UNKNOWN') AS team_or_unknown
    FROM ranked
    WHERE team_level_avg_level < 3.4
)
SELECT DISTINCT level
FROM scored
ORDER BY level
