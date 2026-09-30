WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        ABS(approved_amount - 16404.6) AS approved_amount_distance,
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
enriched AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.approved_amount_distance,
        s.reported_at_month,
        a.adjuster_name,
        a.team AS adjuster_team,
        a.level AS adjuster_level
    FROM cleaned s
    JOIN adjusters a ON a.id = s.adjuster_id
),
adjuster_id_stats AS (
    -- Avg approved_amount_distance per adjuster_id
    SELECT adjuster_id, AVG(approved_amount_distance) AS avg_approved_amount_distance
    FROM enriched
    GROUP BY adjuster_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_approved_amount_distance,
        ROUND(s.approved_amount_distance - g.avg_approved_amount_distance, 2) AS approved_amount_distance_minus_avg
    FROM enriched s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.approved_amount_distance > g.avg_approved_amount_distance
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM vs_group s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.approved_amount >= 2887.3
              AND HOUR(c.reported_at) BETWEEN 7 AND 10
        )
      AND s.policy_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claimed_amount,
    status,
    reported_at,
    approved_amount_distance,
    reported_at_month,
    adjuster_name,
    adjuster_team,
    adjuster_level,
    avg_approved_amount_distance,
    approved_amount_distance_minus_avg,
    CONCAT_WS('|', adjuster_name, adjuster_team) AS adjuster_name_adjuster_team_label
FROM qualified
WHERE approved_amount_distance > 15000.0
ORDER BY adjuster_name NULLS LAST, claim_date DESC
