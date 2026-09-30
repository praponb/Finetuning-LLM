WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE claimed_amount >= 10518.4
      AND adjuster_id >= 4
),
enriched_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.reported_at,
        a.team AS adjuster_team
    FROM filtered_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
)
-- Subtotals with GROUPING SETS
SELECT
    adjuster_id,
    adjuster_team,
    COUNT(*) AS n_rows,
    MIN(approved_amount) AS min_approved_amount,
    SUM(CASE WHEN adjuster_team IS NOT NULL THEN approved_amount ELSE 0 END) AS approved_amount_adjuster_team_is_not_null
FROM enriched_adjuster
GROUP BY GROUPING SETS ((adjuster_id, adjuster_team), (adjuster_id), (adjuster_team))
