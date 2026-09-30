WITH cleaned AS (
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
        YEAR(claim_date) AS claim_date_year,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled_2
    FROM claims
    WHERE approved_amount > 1364.5
),
with_details AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claim_date_year,
        s.claimed_amount_filled,
        s.claimed_amount_filled_2,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date,
        a.team AS adjuster_team
    FROM cleaned s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.claimed_amount_filled,
        s.policy_premium,
        s.policy_start_date,
        s.adjuster_team,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at,
        c.claim_date AS claim_claim_date
    FROM with_details s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
)
SELECT
    adjuster_team,
    status,
    adjuster_id,
    ROUND(AVG(claim_approved_amount), 2) AS avg_claim_approved_amount,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT adjuster_team) AS distinct_adjuster_teams
FROM line_level
GROUP BY CUBE(adjuster_team, status, adjuster_id)
ORDER BY adjuster_team NULLS LAST, status NULLS LAST, adjuster_id NULLS LAST
