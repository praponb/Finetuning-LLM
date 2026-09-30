WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        ABS(claimed_amount - 32441.2) AS claimed_amount_distance
    FROM claims
    WHERE claimed_amount BETWEEN 10673.3 AND 34401.0
      AND HOUR(reported_at) BETWEEN 9 AND 14
),
combined_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.reported_at,
        s.claimed_amount_filled,
        s.claimed_amount_distance,
        a.adjuster_name,
        a.team AS adjuster_team,
        a.level AS adjuster_level
    FROM valid_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
combined_details_2 AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.level AS adjuster_level_2,
        a.adjuster_name AS adjuster_adjuster_name,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date
    FROM combined_details s
    JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT DISTINCT claimed_amount_filled, adjuster_team, adjuster_name
FROM combined_details_2
ORDER BY adjuster_team NULLS LAST
