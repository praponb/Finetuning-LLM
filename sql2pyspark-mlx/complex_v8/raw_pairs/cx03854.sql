WITH base AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        LENGTH(TRIM(channel)) AS channel_len,
        UPPER(TRIM(channel)) AS channel_clean
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-24 08:00:00'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        MAX(event_at) AS last_event_at,
        COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        t.last_event_at,
        COALESCE(t.event_count, 0) AS event_count
    FROM base s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
with_parent AS (
    -- Join support_agents
    SELECT s.*, su.tier AS agent_tier
    FROM summarized_rows s
    JOIN support_agents su ON su.id = s.agent_id AND su.tier = 'T3'
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        SIZE(tags) AS tags_count,
        DATE_TRUNC('YEAR', last_event_at) AS last_event_at_year,
        CASE WHEN csat >= 4 THEN 'A' WHEN csat >= 2 THEN 'B' ELSE 'C' END AS csat_tier
    FROM with_parent
    WHERE csat < 5
),
with_details AS (
    -- Join support_agents
    SELECT
        s.*,
        su.tier AS agent_tier_2,
        su.agent_name,
        su.team AS agent_team
    FROM derived s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    csat,
    tags,
    channel_len,
    channel_clean,
    first_event_at,
    event_count,
    last_event_at_year,
    agent_tier_2,
    agent_name,
    agent_team,
    ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(last_event_at_year)) / 60, 1) AS minutes_last_event_at_year_to_closed_at
FROM with_details
ORDER BY opened_at DESC
