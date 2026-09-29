WITH base AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        tags,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 60, 1) AS minutes_closed_at_to_opened_at
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-12 10:00:00'
      AND requester_id >= 7
),
unique_rows AS (
    -- Keep the latest row per agent_id, channel
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        tags,
        minutes_closed_at_to_opened_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, channel ORDER BY minutes_closed_at_to_opened_at DESC, id DESC) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
),
filtered AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM unique_rows s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.channel = 'email'
              AND t.csat BETWEEN 2 AND 5
        )
),
agent_id_stats AS (
    -- Median minutes_closed_at_to_opened_at per agent_id
    SELECT agent_id, PERCENTILE_APPROX(minutes_closed_at_to_opened_at, 0.5) AS median_minutes_closed_at_to_opened_at
    FROM filtered
    GROUP BY agent_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_minutes_closed_at_to_opened_at,
        ROUND(s.minutes_closed_at_to_opened_at - g.median_minutes_closed_at_to_opened_at, 2) AS minutes_closed_at_to_opened_at_minus_median
    FROM filtered s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.minutes_closed_at_to_opened_at >= g.median_minutes_closed_at_to_opened_at
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(CASE WHEN priority = 'P2' THEN 1 END) AS ticket_priority_count,
        COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_priority_count, 0) AS ticket_priority_count,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM compared s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
combined_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.tags,
        s.minutes_closed_at_to_opened_at,
        s.median_minutes_closed_at_to_opened_at,
        s.minutes_closed_at_to_opened_at_minus_median,
        s.ticket_priority_count,
        s.ticket_count,
        su.hired_on AS agent_hired_on
    FROM profiled s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_parent_agent AS (
    -- Join support_agents
    SELECT s.*, su.team AS agent_team
    FROM combined_details s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    agent_id,
    opened_at,
    tags,
    minutes_closed_at_to_opened_at,
    median_minutes_closed_at_to_opened_at,
    minutes_closed_at_to_opened_at_minus_median,
    ticket_priority_count,
    ticket_count,
    agent_hired_on,
    agent_team
FROM with_parent_agent
ORDER BY minutes_closed_at_to_opened_at_minus_median DESC
