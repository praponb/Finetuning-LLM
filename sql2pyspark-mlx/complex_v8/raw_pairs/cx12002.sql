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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY priority ORDER BY event_at DESC NULLS FIRST, id) AS event_at_rank,
        NTH_VALUE(hours_since_open, 2) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hours_since_open,
        ROUND(hours_since_open / NULLIF(SUM(hours_since_open) OVER (PARTITION BY event_type, agent_id), 0), 4) AS hours_since_open_share_of_event_type_agent_id,
        SUM(hours_since_open) OVER (PARTITION BY event_type, agent_id) AS event_type_agent_id_sum_hours_since_open,
        NTILE(4) OVER (PARTITION BY priority ORDER BY event_at DESC NULLS FIRST, id) AS event_at_quartile
    FROM covered
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, t.csat AS ticket_csat, su.team AS agent_team
    FROM with_windows s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
    WHERE t.csat > 3
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    agent_id,
    event_at_rank,
    second_hours_since_open,
    event_type_agent_id_sum_hours_since_open,
    event_at_quartile,
    ticket_csat,
    agent_team,
    DATE_TRUNC('MONTH', event_at) AS event_at_month,
    ABS(event_at_rank - 6) AS event_at_rank_distance
FROM enriched
ORDER BY ticket_csat DESC, event_type
