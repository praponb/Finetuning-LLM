WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        CONCAT_WS(': ', priority, channel) AS priority_channel_label,
        QUARTER(closed_at) AS closed_at_quarter
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-03-08 10:00:00'
      AND closed_at < TIMESTAMP '2025-02-27 06:00:00'
),
without_matches AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM base_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.closed_at >= TIMESTAMP '2025-01-25 11:00:00'
        )
      AND s.priority_channel_label IS NOT NULL
),
latest AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        priority_channel_label,
        closed_at_quarter
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY closed_at_quarter DESC, id DESC) AS recency_rank
        FROM without_matches
    ) ranked
    WHERE recency_rank = 1
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(requester_id, closed_at_quarter) AS least_requester_id_closed_at_quarter,
        ABS(requester_id - 16) AS requester_id_distance,
        DATE_TRUNC('MONTH', closed_at) AS closed_at_month
    FROM latest
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    channel,
    tags,
    priority_channel_label,
    closed_at_quarter,
    least_requester_id_closed_at_quarter,
    closed_at_month,
    DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_label,
    CASE WHEN channel IN ('email', 'chat') THEN 'group_a' WHEN channel = 'phone' THEN 'group_b' ELSE 'other' END AS channel_group
FROM labeled
