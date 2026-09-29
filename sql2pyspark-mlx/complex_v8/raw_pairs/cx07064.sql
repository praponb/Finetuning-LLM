WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        LENGTH(TRIM(priority)) AS priority_len,
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_label,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-02-22 12:00:00'
),
enriched AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.opened_at_label,
        s.priority_or_unknown,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM cleaned_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.csat,
        s.opened_at_label,
        s.priority_or_unknown,
        s.agent_team,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor
    FROM enriched s
    JOIN ticket_events t ON t.ticket_id = s.id
),
event_actor_stats AS (
    -- Avg csat per event_actor
    SELECT event_actor, AVG(csat) AS avg_csat
    FROM detail_rows_events
    GROUP BY event_actor
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_csat, ROUND(s.csat - g.avg_csat, 2) AS csat_minus_avg
    FROM detail_rows_events s
    JOIN event_actor_stats g ON g.event_actor = s.event_actor
    WHERE s.csat > g.avg_csat
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        COUNT(CASE WHEN priority = 'P4' THEN 1 END) AS ticket_priority_count
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        COALESCE(t.ticket_priority_count, 0) AS ticket_priority_count
    FROM outliers s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
deduped AS (
    -- Keep the latest row per agent_id, priority_or_unknown
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        csat,
        opened_at_label,
        priority_or_unknown,
        agent_team,
        event_id,
        event_type,
        event_actor,
        avg_csat,
        csat_minus_avg,
        ticket_count,
        ticket_priority_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, priority_or_unknown ORDER BY avg_csat DESC, id DESC, event_id DESC) AS recency_rank
        FROM with_stats
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    channel,
    csat,
    priority_or_unknown,
    agent_team,
    event_id,
    event_type,
    event_actor,
    avg_csat,
    csat_minus_avg,
    LOWER(TRIM(event_actor)) AS event_actor_clean,
    CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref
FROM deduped
ORDER BY id
