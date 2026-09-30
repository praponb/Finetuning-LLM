WITH prepared_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        INITCAP(TRIM(team)) AS team_clean
    FROM adjusters
    WHERE team NOT IN ('Travel', 'Property')
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.id) AS claim_count
    FROM prepared_adjusters s
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.team,
        s.level,
        s.team_clean,
        s.claim_count,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claimed_amount AS claim_claimed_amount,
        c.approved_amount AS claim_approved_amount
    FROM profiled s
    JOIN claims c ON c.adjuster_id = s.id
)
SELECT
    id,
    level,
    claim_count,
    claim_id,
    claim_reported_at,
    DATE_TRUNC('WEEK', claim_reported_at) AS claim_reported_at_week,
    SUBSTRING(TRIM(team), 1, 3) AS team_prefix
FROM with_children
ORDER BY claim_reported_at_week NULLS LAST, claim_id, id
LIMIT 5
