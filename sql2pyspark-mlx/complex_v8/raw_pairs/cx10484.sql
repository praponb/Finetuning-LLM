WITH scoped AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        UPPER(TRIM(team)) AS team_clean,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown
    FROM adjusters
    WHERE level <= 3
      AND team = 'Health'
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MIN(claim_date) AS first_claim_date,
        MIN(reported_at) AS first_claim_reported_at,
        COUNT(*) AS claim_count
    FROM claims
    GROUP BY adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_date,
        c.first_claim_reported_at,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM scoped s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.id
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
              AND c.claim_date BETWEEN DATE '2024-03-15' AND DATE '2025-11-25'
              AND c.status IN ('REJECTED', 'IN_REVIEW', 'PAID')
        )
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_name,
        level,
        team_or_unknown,
        first_claim_date,
        first_claim_reported_at,
        claim_count,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY team_or_unknown, level), 0), 4) AS level_share_of_team_or_unknown_level,
        FIRST_VALUE(first_claim_date) OVER (ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_first_claim_date
    FROM qualified
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT first_claim_date, first_claim_reported_at, team_or_unknown, level
    FROM windowed
) src
PIVOT (
    MIN(level) AS min_v, COUNT(level) AS n
    FOR team_or_unknown IN ('Motor', 'Property')
)
ORDER BY first_claim_date, first_claim_reported_at
