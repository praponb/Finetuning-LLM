WITH covered AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        agent_id,
        hours_since_open,
        LENGTH(TRIM(priority)) AS priority_len
    FROM covered
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        agent_id,
        hours_since_open,
        priority_len,
        LAG(hours_since_open, 3) OVER (PARTITION BY agent_id ORDER BY hours_since_open DESC, id) AS prev3_hours_since_open,
        ROUND(hours_since_open - AVG(hours_since_open) OVER (PARTITION BY priority_len), 2) AS hours_since_open_vs_priority_len_avg,
        ROUND(PERCENT_RANK() OVER w_agent_id_by_hours_since_open_2, 3) AS hours_since_open_percent_rank,
        MAX(hours_since_open) OVER (PARTITION BY priority_len) AS priority_len_max_hours_since_open
    FROM enriched
    WINDOW w_agent_id_by_hours_since_open_2 AS (PARTITION BY agent_id ORDER BY hours_since_open DESC)
),
exploded_detail_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.agent_id,
        s.hours_since_open,
        s.priority_len,
        s.prev3_hours_since_open,
        s.hours_since_open_vs_priority_len_avg,
        s.hours_since_open_percent_rank,
        t.id AS ticket_id,
        t.priority AS ticket_priority,
        t.csat AS ticket_csat,
        t.channel AS ticket_channel,
        t.closed_at AS ticket_closed_at
    FROM metrics s
    JOIN tickets t ON t.agent_id = s.agent_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(ticket_channel, 'ALL') AS ticket_channel,
    agent_id,
    PERCENTILE_APPROX(hours_since_open_vs_priority_len_avg, 0.5) AS median_hours_since_open_vs_priority_len_avg,
    ROUND(AVG(hours_since_open_vs_priority_len_avg), 2) AS avg_hours_since_open_vs_priority_len_avg
FROM exploded_detail_tickets
GROUP BY GROUPING SETS ((ticket_channel, agent_id), (ticket_channel), (agent_id))
