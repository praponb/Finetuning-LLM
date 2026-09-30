WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour
    FROM ticket_events
    WHERE event_type = 'reopened'
      AND ticket_id >= 48
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT s.*, t.agent_id AS ticket_agent_id
    FROM scoped_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.agent_id IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_at_hour,
        NTH_VALUE(event_at_hour, 2) OVER (ORDER BY event_at_hour ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_hour,
        LAG(event_at) OVER w_ticket_agent_id_event_at_to AS prev_event_at,
        LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at_hour, id) AS prev_event_at_2,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_at_hour), 3) AS event_at_hour_cume_dist
    FROM with_details_ticket
    WINDOW w_ticket_agent_id_event_at_to AS (PARTITION BY ticket_agent_id, TO_DATE(event_at) ORDER BY event_at_hour DESC, id)
),
joined AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.second_event_at_hour,
        s.prev_event_at_2,
        s.event_at_hour_cume_dist,
        t.opened_at AS ticket_opened_at,
        t.tags AS ticket_tags
    FROM windowed s
    JOIN tickets t ON t.id = s.ticket_id
),
latest_per_key AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        second_event_at_hour,
        prev_event_at_2,
        event_at_hour_cume_dist,
        ticket_opened_at,
        ticket_tags
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY second_event_at_hour DESC, id DESC) AS recency_rank
        FROM joined
    ) ranked
    WHERE recency_rank = 1
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM latest_per_key
    WHERE event_at_hour_cume_dist < (SELECT MAX(event_at_hour_cume_dist) FROM latest_per_key)
)
SELECT
    id,
    ticket_id,
    event_type,
    second_event_at_hour,
    prev_event_at_2,
    event_at_hour_cume_dist,
    ticket_opened_at,
    ticket_tags
FROM filtered
