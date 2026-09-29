WITH cleaned AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE level >= 3
      AND team IS NOT NULL
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LOWER(REGEXP_REPLACE(TRIM(adjuster_name), ' ', '_')) AS adjuster_name_slug,
        CASE WHEN team IN ('Health', 'Property') THEN 'group_a' WHEN team = 'Motor' THEN 'group_b' ELSE 'other' END AS team_group
    FROM cleaned
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, COUNT(*) AS claim_count, MAX(claim_date) AS last_claim_date
    FROM claims
    GROUP BY adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.last_claim_date
    FROM enriched s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.id
),
labeled AS (
    -- Add calculated columns
    SELECT *, LEAST(claim_count, level) AS least_claim_count_level
    FROM with_stats
)
-- Subtotals with ROLLUP
SELECT
    least_claim_count_level,
    team,
    GROUPING_ID() AS gid,
    SUM(claim_count) AS total_claim_count
FROM labeled
GROUP BY ROLLUP(least_claim_count_level, team)
ORDER BY least_claim_count_level NULLS LAST, team NULLS LAST
