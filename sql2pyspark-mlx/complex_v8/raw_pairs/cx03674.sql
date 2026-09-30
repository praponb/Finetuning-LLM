WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        INITCAP(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_clean,
        t.csat AS ticket_csat,
        t.opened_at AS ticket_opened_at,
        t.closed_at AS ticket_closed_at,
        su.tier AS agent_tier
    FROM filtered_events s
    JOIN tickets t ON t.id = s.ticket_id AND t.closed_at >= TIMESTAMP '2025-01-20 22:00:00'
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_actor_ticket_opened_at_month AS rows_in_actor_ticket_opened_at_month,
        NTILE(3) OVER w_agent_tier_ticket_id_by_tick AS ticket_closed_at_tercile,
        MIN(ticket_csat) OVER (PARTITION BY actor, MONTH(ticket_opened_at)) AS actor_ticket_opened_at_month_min_ticket_csat
    FROM joined_ticket
    WINDOW w_actor_ticket_opened_at_month AS (PARTITION BY actor, MONTH(ticket_opened_at)), w_agent_tier_ticket_id_by_tick AS (PARTITION BY agent_tier, ticket_id ORDER BY ticket_closed_at, id)
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    event_type_clean,
    ticket_csat,
    ticket_opened_at,
    agent_tier,
    rows_in_actor_ticket_opened_at_month,
    actor_ticket_opened_at_month_min_ticket_csat
FROM ranked
ORDER BY event_type_clean DESC, actor DESC NULLS FIRST
