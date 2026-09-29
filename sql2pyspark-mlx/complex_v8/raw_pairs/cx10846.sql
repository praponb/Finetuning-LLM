WITH valid_adjusters AS (
    -- Scope the adjusters rows
    SELECT *
    FROM adjusters
    WHERE (level < 3 OR team NOT IN ('Health', 'Property'))
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(adjuster_name), ' ', '_')) AS adjuster_name_slug,
        LENGTH(TRIM(adjuster_name)) AS adjuster_name_len,
        CONCAT_WS('-', adjuster_name, team) AS adjuster_name_team_label
    FROM valid_adjusters
    WHERE UPPER(TRIM(adjuster_name)) LIKE 'H%'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(level, 2) OVER (PARTITION BY team, adjuster_name_len ORDER BY adjuster_name_len, id) AS prev2_level,
        MAX(adjuster_name_len) OVER (PARTITION BY team, adjuster_name_len ORDER BY adjuster_name_len, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS adjuster_name_len_max_1_prior
    FROM tagged
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM ranked s
    WHERE s.id IN (
            SELECT c.adjuster_id
            FROM claims c
            WHERE c.approved_amount BETWEEN 0.0 AND 14993.0
        )
)
SELECT id, metric_name, reading
FROM (
    SELECT id, adjuster_name_len, prev2_level, level
    FROM screened
) src
UNPIVOT (
    reading FOR metric_name IN (adjuster_name_len, prev2_level, level)
) unp
ORDER BY id, metric_name
