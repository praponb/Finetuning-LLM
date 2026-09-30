WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IN ('created', 'escalated')
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id) AS event_at_quartile,
        LAG(event_at) OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id) AS prev_event_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS event_at_seq,
        ROW_NUMBER() OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id) AS rn,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_ticket_id_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY actor, ticket_id ORDER BY event_at), 3) AS event_at_cume_dist
    FROM prepared_events
    WINDOW w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at, id)
),
deduped AS (
    -- Keep the latest row per actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_quartile,
        prev_event_at,
        event_at_seq,
        rn,
        minutes_since_prev_event_at,
        event_at_cume_dist
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor ORDER BY rn DESC, id DESC) AS rn_2
        FROM with_trends
    ) ranked
    WHERE rn_2 = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_at_quartile, actor, minutes_since_prev_event_at
    FROM deduped
) src
PIVOT (
    SUM(minutes_since_prev_event_at)
    FOR actor IN ('agent', 'system', 'customer')
)
