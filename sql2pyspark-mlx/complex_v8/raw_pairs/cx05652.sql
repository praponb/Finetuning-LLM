WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix,
        LENGTH(TRIM(actor)) AS actor_len
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 8 AND 12
      AND actor <> 'system'
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_prefix,
        t.tags AS ticket_tags
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE ARRAY_CONTAINS(t.tags, 'outage')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type_prefix,
        ticket_tags,
        NTILE(5) OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS event_at_quintile,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at, id) AS event_at_tercile,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROW_NUMBER() OVER w_event_type_by_event_at AS rn,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor, ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_2,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at, id) AS event_at_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        LAG(event_at, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at, id) AS prev3_event_at,
        LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id) AS prev_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(CUME_DIST() OVER w_event_at_month_by_event_at_t, 3) AS event_at_cume_dist_2,
        NTILE(10) OVER w_actor_ticket_id_by_event_at_2 AS event_at_decile,
        FIRST_VALUE(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_2
    FROM with_parent
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at DESC, id), w_event_at_month_by_event_at_t AS (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at), w_actor_ticket_id_by_event_at_2 AS (PARTITION BY actor, ticket_id ORDER BY event_at ASC, id)
)
-- Final projection
SELECT
    id,
    event_at,
    event_at_quintile,
    event_at_tercile,
    first_event_at,
    last_known_event_at,
    last_known_event_at_2,
    event_at_seq,
    event_at_cume_dist,
    prev3_event_at,
    minutes_since_prev_event_at,
    event_at_cume_dist_2,
    event_at_decile,
    first_event_at_2,
    event_at + INTERVAL 2 HOURS AS event_at_plus_2h,
    GREATEST(rn, event_at_seq) AS greatest_rn_event_at_seq
FROM ranked
WHERE first_event_at < TIMESTAMP '2025-03-02 09:00:00'
