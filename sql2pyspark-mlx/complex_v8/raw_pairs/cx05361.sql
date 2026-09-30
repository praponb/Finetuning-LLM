WITH prepared_adjusters AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown,
        CONCAT(team, '#', CAST(id AS STRING)) AS team_ref
    FROM adjusters
    WHERE team IS NOT NULL
      AND level >= 3
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.id) AS first_claim_reported_at
    FROM prepared_adjusters s
)
SELECT
    level,
    team,
    team_or_unknown,
    GROUPING(level) AS is_total_level,
    COUNT(*) FILTER (WHERE adjuster_name IS NOT NULL) AS n_adjuster_name_is_not_null
FROM with_counts
GROUP BY CUBE(level, team, team_or_unknown)
ORDER BY level NULLS LAST, team NULLS LAST, team_or_unknown NULLS LAST
