WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month
    FROM claims
),
status_stats AS (
    -- Avg claimed_amount per status
    SELECT status, AVG(claimed_amount) AS avg_claimed_amount
    FROM prepared_claims
    GROUP BY status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claimed_amount,
        ROUND(s.claimed_amount - g.avg_claimed_amount, 2) AS claimed_amount_minus_avg
    FROM prepared_claims s
    JOIN status_stats g ON g.status = s.status
    WHERE s.claimed_amount > g.avg_claimed_amount
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM above_average s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.approved_amount <= 17759.3
              AND c.claim_date IS NOT NULL
        )
      AND s.policy_id <= 14
)
-- Subtotals with CUBE
SELECT
    status,
    policy_id,
    GROUPING_ID() AS gid,
    COUNT(*) FILTER (WHERE status IS NOT NULL) AS n_status_is_not_null
FROM without_matches
GROUP BY CUBE(status, policy_id)
