WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        reported_at + INTERVAL 30 MINUTES AS reported_at_plus_30m
    FROM claims
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY status)) AS distinct_adjuster_id_in_status,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY status), 0), 4) AS claimed_amount_share_of_status,
        MIN(claimed_amount) OVER w_status AS status_min_claimed_amount
    FROM prepared
    WINDOW w_status AS (PARTITION BY status)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        reported_at_plus_30m,
        rows_in_status,
        distinct_adjuster_id_in_status,
        claimed_amount_share_of_status,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY status ORDER BY rows_in_status DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at,
        FIRST_VALUE(claimed_amount) OVER (PARTITION BY claim_type, status ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount
    FROM metrics
),
adjuster_id_stats AS (
    -- Avg claimed_amount_share_of_status per adjuster_id
    SELECT adjuster_id, AVG(claimed_amount_share_of_status) AS avg_claimed_amount_share_of_status
    FROM with_windows
    GROUP BY adjuster_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claimed_amount_share_of_status,
        ROUND(s.claimed_amount_share_of_status - g.avg_claimed_amount_share_of_status, 2) AS claimed_amount_share_of_status_minus_avg
    FROM with_windows s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.claimed_amount_share_of_status > g.avg_claimed_amount_share_of_status
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM above_average s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.status IN ('OPEN', 'APPROVED')
        )
      AND s.adjuster_id <= 5
)
SELECT
    id,
    adjuster_id,
    claimed_amount,
    approved_amount,
    status,
    rows_in_status,
    distinct_adjuster_id_in_status,
    claimed_amount_share_of_status,
    first_known_reported_at,
    first_claimed_amount,
    avg_claimed_amount_share_of_status,
    claimed_amount_share_of_status_minus_avg
FROM with_activity
ORDER BY approved_amount DESC NULLS FIRST
