WITH recent_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status,
        reported_at,
        UPPER(TRIM(claim_type)) AS claim_type_clean
    FROM claims
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        adjuster_id,
        COUNT(*) FILTER (WHERE claim_type = 'flood') AS n_claim_type_flood,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids
    FROM recent_claims
    GROUP BY 1, 2
),
enriched_adjuster AS (
    -- Join adjusters
    SELECT s.*, a.adjuster_name, a.team AS adjuster_team
    FROM totals s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.level >= 3
)
SELECT
    status,
    n_claim_type_flood,
    adjuster_team,
    CONCAT_WS('-', status, adjuster_team) AS status_adjuster_team_label
FROM enriched_adjuster
ORDER BY adjuster_team
