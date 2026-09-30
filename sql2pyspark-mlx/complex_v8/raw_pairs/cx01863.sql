WITH valid AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CONCAT(team, '#', CAST(level AS STRING)) AS team_ref
    FROM adjusters
    WHERE level > 1
      AND UPPER(TRIM(adjuster_name)) LIKE 'E%'
),
scored AS (
    -- Label and score rows
    SELECT *, LENGTH(TRIM(adjuster_name)) AS adjuster_name_len
    FROM valid
    WHERE adjuster_name IS NOT NULL
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, COUNT(*) AS claim_count, MIN(claim_date) AS first_claim_date
    FROM claims
    GROUP BY adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.first_claim_date
    FROM scored s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.id
),
claim_stats_2 AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, MIN(claimed_amount) AS min_claim_claimed_amount
    FROM claims
    GROUP BY adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.min_claim_claimed_amount
    FROM summarized_rows s
    LEFT JOIN claim_stats_2 c ON c.adjuster_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(team, 'ALL') AS team,
    level,
    GROUPING_ID() AS gid,
    SUM(CASE WHEN adjuster_name IS NOT NULL THEN claim_count ELSE 0 END) AS claim_count_adjuster_name_is_not_null,
    COUNT(*) AS row_count
FROM with_counts
GROUP BY GROUPING SETS ((team, level), (team), ())
ORDER BY team NULLS LAST, level NULLS LAST
