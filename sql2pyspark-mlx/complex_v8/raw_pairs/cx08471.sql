WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-02-12 12:00:00'
      AND claimed_amount < 41878.5
),
with_parent_adjuster AS (
    -- Join adjusters
    SELECT s.*, a.level AS adjuster_level, a.team AS adjuster_team
    FROM base s
    JOIN adjusters a ON a.id = s.adjuster_id
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_parent_adjuster s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.status = 'OPEN'
        )
),
with_activity AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.level >= 2
        )
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_activity
    WHERE adjuster_level > (SELECT AVG(adjuster_level) FROM with_activity)
),
annotated AS (
    -- Add calculated columns
    SELECT *, DATE_TRUNC('DAY', reported_at) AS reported_at_day
    FROM filtered
),
tagged AS (
    -- Label and score rows
    SELECT *, COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM annotated
    WHERE claimed_amount < 33690.0
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    approved_amount,
    reported_at,
    adjuster_level,
    reported_at_day,
    DATEDIFF(DATE '2026-01-01', reported_at) AS days_since_reported_at
FROM tagged
ORDER BY approved_amount DESC
