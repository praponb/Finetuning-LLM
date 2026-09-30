WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        csat,
        tags,
        TO_DATE(closed_at) AS closed_at_date,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
    WHERE (closed_at >= TIMESTAMP '2025-01-17 21:00:00' OR SIZE(tags) >= 2)
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM recent
    WHERE requester_id > (SELECT MIN(requester_id) FROM recent)
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MAX(closed_at) AS last_ticket_closed_at,
        MIN(opened_at) AS first_ticket_opened_at
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.last_ticket_closed_at,
        t.first_ticket_opened_at
    FROM without_matches s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM with_counts s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
        )
)
SELECT
    id,
    requester_id,
    agent_id,
    channel,
    csat,
    closed_at_date,
    priority_or_unknown,
    last_ticket_closed_at,
    LENGTH(TRIM(channel)) AS channel_len,
    COALESCE(csat, 0) AS csat_filled
FROM with_activity
ORDER BY requester_id
