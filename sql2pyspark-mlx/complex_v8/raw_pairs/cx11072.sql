WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE event_type NOT IN ('replied', 'escalated')
      AND ticket_id IS NOT NULL
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type_len,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM valid s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY event_id, event_actor)) AS distinct_ticket_id_in_event_id_event_actor,
        ROW_NUMBER() OVER w_event_actor_event_type_len_b AS event_type_len_seq,
        LEAD(event_type_len, 2) OVER (PARTITION BY event_actor, event_type_len ORDER BY event_type_len DESC NULLS FIRST, id, event_id) AS next2_event_type_len
    FROM line_level_events
    WINDOW w_event_actor_event_type_len_b AS (PARTITION BY event_actor, event_type_len ORDER BY event_type_len DESC NULLS FIRST, id, event_id)
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM metrics
    WHERE distinct_ticket_id_in_event_id_event_actor > (SELECT MIN(distinct_ticket_id_in_event_id_event_actor) FROM metrics)
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at_2
    FROM with_activity s
),
combined_details_event AS (
    -- Attach event attributes
    SELECT s.*, t.event_type, t.actor AS event_actor_2
    FROM profiled s
    JOIN ticket_events t ON t.id = s.event_id
)
SELECT *
FROM (
    SELECT event_actor, event_id, event_type, next2_event_type_len
    FROM combined_details_event
) src
PIVOT (
    MAX(next2_event_type_len)
    FOR event_type IN ('replied', 'escalated')
)
