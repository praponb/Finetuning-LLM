WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at
    FROM claims
    WHERE (claim_date IS NOT NULL OR status NOT IN ('REJECTED', 'APPROVED'))
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.reported_at,
        a.level AS adjuster_level,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM recent_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.status = 'APPROVED'
              AND c.claim_type IS NOT NULL
        )
),
combined_details AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.reported_at,
        s.adjuster_level,
        p.start_date AS policy_start_date,
        a.level AS adjuster_level_2
    FROM qualified s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT *
FROM (
    SELECT adjuster_level, claim_date, claim_type, adjuster_level_2
    FROM combined_details
) src
PIVOT (
    SUM(adjuster_level_2) AS amount, MAX(adjuster_level_2) AS max_v
    FOR claim_type IN ('flood', 'theft')
)
