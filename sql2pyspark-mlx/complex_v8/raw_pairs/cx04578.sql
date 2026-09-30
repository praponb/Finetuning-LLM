WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('DAY', event_at) AS event_at_day,
        UPPER(TRIM(actor)) AS actor_clean,
        LOWER(TRIM(actor)) AS actor_clean_2
    FROM ticket_events
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM scoped_events s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND HOUR(t.event_at) BETWEEN 9 AND 14
              AND t.event_type IS NOT NULL
        )
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor_clean_2,
        t.channel AS ticket_channel,
        t.opened_at AS ticket_opened_at,
        t.closed_at AS ticket_closed_at,
        su.tier AS agent_tier
    FROM without_matches s
    JOIN tickets t ON t.id = s.ticket_id AND ARRAY_CONTAINS(t.tags, 'login')
    JOIN support_agents su ON su.id = t.agent_id
    WHERE HOUR(t.opened_at) BETWEEN 9 AND 14
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor_clean_2,
    ticket_channel,
    ticket_opened_at,
    agent_tier,
    SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
FROM joined_ticket
ORDER BY ticket_opened_at DESC, agent_tier
