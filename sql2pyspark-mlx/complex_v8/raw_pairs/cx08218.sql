WITH scoped AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        DATE_ADD(claim_date, 365) AS claim_date_plus_365d,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount
    FROM claims
    WHERE policy_id IS NOT NULL
      AND claimed_amount > 7204.1
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.claim_date_plus_365d,
        s.status_or_unknown,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM scoped s
    JOIN adjusters a ON a.id = s.adjuster_id
)
-- Subtotals with GROUPING SETS
SELECT
    adjuster_id,
    policy_id,
    COUNT(DISTINCT adjuster_team) AS distinct_adjuster_teams
FROM combined_details
GROUP BY GROUPING SETS ((adjuster_id, policy_id), (adjuster_id), (policy_id))
ORDER BY adjuster_id NULLS LAST, policy_id NULLS LAST
