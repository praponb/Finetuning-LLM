WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount
    FROM claims
    WHERE approved_amount >= 1364.5
      AND YEAR(claim_date) = 2025
),
adjuster_id_stats AS (
    -- Avg approved_amount per adjuster_id
    SELECT adjuster_id, AVG(approved_amount) AS avg_approved_amount
    FROM valid
    GROUP BY adjuster_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_approved_amount,
        ROUND(s.approved_amount - g.avg_approved_amount, 2) AS approved_amount_minus_avg
    FROM valid s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.approved_amount > g.avg_approved_amount
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.avg_approved_amount,
        c.id AS claim_id,
        c.claim_date AS claim_claim_date,
        c.reported_at AS claim_reported_at
    FROM above_average s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
combined_details_claim AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.claim_id,
        s.claim_reported_at,
        c.approved_amount AS claim_approved_amount,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at_2,
        a.team AS adjuster_team,
        a.level AS adjuster_level
    FROM with_children_claims s
    LEFT JOIN claims c ON c.id = s.claim_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    claim_id,
    adjuster_team,
    adjuster_id,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT claim_status) AS distinct_claim_status,
    ROUND(STDDEV(claim_approved_amount), 2) AS stddev_claim_approved_amount
FROM combined_details_claim
GROUP BY GROUPING SETS ((claim_id, adjuster_team, adjuster_id), (claim_id), ())
