WITH valid_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        QUARTER(claim_date) AS claim_date_quarter,
        YEAR(claim_date) AS claim_date_year
    FROM claims
    WHERE (adjuster_id IS NOT NULL OR MONTH(reported_at) IN (1, 2))
),
enriched_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.claim_date_year,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM valid_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
filtered AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM enriched_adjuster s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.level <= 3
        )
),
latest AS (
    -- Keep the latest row per adjuster_team
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        claim_date_year,
        adjuster_level,
        adjuster_team,
        adjuster_name
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_team ORDER BY adjuster_level DESC, id DESC) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
with_parent_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date_year,
        s.adjuster_level,
        s.adjuster_team,
        s.adjuster_name,
        a.adjuster_name AS adjuster_adjuster_name,
        a.team AS adjuster_team_2,
        a.level AS adjuster_level_2,
        p.start_date AS policy_start_date,
        p.premium AS policy_premium
    FROM latest s
    JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date_year,
    adjuster_level,
    adjuster_team,
    adjuster_name,
    adjuster_adjuster_name,
    adjuster_team_2,
    adjuster_level_2,
    policy_start_date,
    policy_premium
FROM with_parent_adjuster
ORDER BY adjuster_adjuster_name DESC, id
LIMIT 10
