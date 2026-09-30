WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        ROUND(claimed_amount * 1.07, 2) AS claimed_amount_adj,
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount
    FROM claims
    WHERE claimed_amount > 18364.1
      AND adjuster_id IS NOT NULL
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.claimed_amount_filled,
        s.claimed_amount_adj,
        s.greatest_claimed_amount_approved_amount,
        a.team AS adjuster_team,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM base_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
combined_details AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.status,
        s.claimed_amount_filled,
        s.claimed_amount_adj,
        s.greatest_claimed_amount_approved_amount,
        s.adjuster_team,
        s.adjuster_name,
        s.adjuster_level,
        p.holder_id AS policy_holder_id
    FROM with_parent s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.holder_id >= 15
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM combined_details
    WHERE adjuster_level > (SELECT MIN(adjuster_level) FROM combined_details)
),
adjuster_team_stats AS (
    -- Median claimed_amount_filled per adjuster_team
    SELECT adjuster_team, PERCENTILE_APPROX(claimed_amount_filled, 0.5) AS median_claimed_amount_filled
    FROM screened
    GROUP BY adjuster_team
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_claimed_amount_filled,
        ROUND(s.claimed_amount_filled - g.median_claimed_amount_filled, 2) AS claimed_amount_filled_minus_median
    FROM screened s
    JOIN adjuster_team_stats g ON g.adjuster_team = s.adjuster_team
    WHERE s.claimed_amount_filled >= g.median_claimed_amount_filled
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount_filled,
        s.claimed_amount_adj,
        s.greatest_claimed_amount_approved_amount,
        s.adjuster_level,
        s.policy_holder_id,
        s.claimed_amount_filled_minus_median,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date
    FROM vs_group s
    LEFT JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount_filled,
    claimed_amount_adj,
    greatest_claimed_amount_approved_amount,
    adjuster_level,
    policy_holder_id,
    claimed_amount_filled_minus_median,
    policy_premium,
    policy_end_date,
    ROUND(claimed_amount_filled * 0.9, 2) AS claimed_amount_filled_adj,
    DATE_FORMAT(policy_end_date, 'MMM yyyy') AS policy_end_date_label
FROM enriched
WHERE adjuster_id <= 5
ORDER BY claimed_amount_filled, claimed_amount_filled_minus_median NULLS LAST
