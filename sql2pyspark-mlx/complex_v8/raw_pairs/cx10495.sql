WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        event_at + INTERVAL 1 DAYS AS event_at_plus_1d
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(event_at_plus_1d) - UNIX_TIMESTAMP(LAG(event_at_plus_1d) OVER (PARTITION BY event_type_or_unknown, event_type ORDER BY event_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_event_at_plus_1d,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type_or_unknown, event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY event_type_or_unknown, event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LAG(event_at, 3) OVER (PARTITION BY event_type_or_unknown, event_type ORDER BY event_at ASC, id) AS prev3_event_at,
        LEAD(event_at_plus_1d, 2) OVER (PARTITION BY event_type_or_unknown, event_type ORDER BY event_at NULLS LAST, id) AS next2_event_at_plus_1d
    FROM scoped_events
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_or_unknown,
        s.actor_slug,
        s.event_at_plus_1d,
        s.minutes_since_prev_event_at_plus_1d,
        s.second_event_at,
        s.last_event_at,
        s.prev3_event_at,
        t.csat AS ticket_csat
    FROM sequenced s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
filtered AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM with_details s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.priority = 'P1'
        )
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.priority AS ticket_priority,
        t.requester_id AS ticket_requester_id,
        su.agent_name
    FROM filtered s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.opened_at < TIMESTAMP '2025-02-24 09:00:00'
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_activity AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM combined_details_ticket s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.priority IS NOT NULL
              AND t.csat <= 4
        )
)
SELECT *
FROM (
    SELECT event_type, last_event_at, actor, minutes_since_prev_event_at_plus_1d
    FROM with_activity
) src
PIVOT (
    SUM(minutes_since_prev_event_at_plus_1d)
    FOR actor IN ('customer', 'agent', 'system')
)
