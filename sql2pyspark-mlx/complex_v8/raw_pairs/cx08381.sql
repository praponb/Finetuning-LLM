WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id >= 22
      AND actor <> 'system'
),
with_activity AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM prepared_events s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.channel IN ('phone', 'email', 'web')
        )
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        event_at + INTERVAL 2 HOURS AS event_at_plus_2h,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM with_activity
    WHERE ticket_id <= 4
),
screened AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.channel IN ('web', 'chat')
        )
),
screened_2 AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM screened s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        actor,
        event_at_plus_2h,
        LEAD(event_at_plus_2h, 3) OVER (ORDER BY event_at_plus_2h ASC, id) AS next3_event_at_plus_2h,
        LAST_VALUE(event_at) OVER (PARTITION BY actor, actor_or_unknown ORDER BY event_at_plus_2h, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor, actor_or_unknown ORDER BY event_at_plus_2h, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROW_NUMBER() OVER (PARTITION BY actor ORDER BY event_at DESC, id) AS rn,
        LAG(event_at) OVER w_actor_by_event_at AS prev_event_at
    FROM screened_2
    WINDOW w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at DESC, id)
)
SELECT
    id,
    event_at,
    event_at_plus_2h,
    last_event_at,
    rn,
    prev_event_at,
    LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
    ROUND((UNIX_TIMESTAMP(prev_event_at) - UNIX_TIMESTAMP(last_event_at)) / 60, 1) AS minutes_last_event_at_to_prev_event_at
FROM sequenced
ORDER BY event_at DESC NULLS FIRST, actor_slug DESC, id
LIMIT 10
