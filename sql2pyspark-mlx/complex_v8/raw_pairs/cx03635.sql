WITH valid_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        CASE WHEN premium >= 4670.9 THEN 'A' WHEN premium >= 2033.8 THEN 'B' ELSE 'C' END AS premium_tier,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE end_date < DATE '2026-03-13'
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.premium_tier,
        s.premium_bucket,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.adjuster_id AS claim_adjuster_id
    FROM valid_policys s
    JOIN claims c ON c.policy_id = s.id AND c.approved_amount > 0.0
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MIN(reported_at) AS first_claim_reported_at,
        ROUND(AVG(claimed_amount), 2) AS avg_claim_claimed_amount
    FROM claims
    GROUP BY adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_reported_at,
        c.avg_claim_claimed_amount
    FROM exploded_detail_claims s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.claim_adjuster_id
),
enriched_adjuster AS (
    -- Join adjusters
    SELECT
        s.*,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM with_counts s
    LEFT JOIN adjusters a ON a.id = s.claim_adjuster_id
)
SELECT
    claim_id,
    claim_adjuster_id,
    adjuster_team,
    GROUPING_ID() AS grouping_level,
    SUM(premium) FILTER (WHERE adjuster_team NOT IN ('Property', 'Travel')) AS premium_adjuster_team_not_in_propert
FROM enriched_adjuster
GROUP BY CUBE(claim_id, claim_adjuster_id, adjuster_team)
