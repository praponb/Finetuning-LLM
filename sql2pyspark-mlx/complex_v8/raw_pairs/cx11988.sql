WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE actor IN ('customer', 'agent')
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.*,
        t.tags AS ticket_tags,
        t.priority AS ticket_priority,
        t.channel AS ticket_channel
    FROM recent_events s
    JOIN tickets t ON t.id = s.ticket_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_priority ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        NTILE(10) OVER (PARTITION BY ticket_priority ORDER BY event_at NULLS LAST, id) AS event_at_decile,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        LAG(event_at) OVER (PARTITION BY ticket_priority ORDER BY event_at NULLS LAST, id) AS prev_event_at,
        LAG(event_at, 3) OVER (PARTITION BY ticket_priority ORDER BY event_at NULLS LAST, id) AS prev3_event_at,
        LEAD(event_at) OVER (ORDER BY event_at, id) AS next_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        RANK() OVER (ORDER BY event_at) AS event_at_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_priority ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor ORDER BY event_at ASC), 3) AS event_at_percent_rank,
        LEAD(event_at) OVER (PARTITION BY actor ORDER BY event_at ASC, id) AS next_event_at_2,
        ROW_NUMBER() OVER (PARTITION BY ticket_priority ORDER BY event_at NULLS LAST, id) AS rn
    FROM with_details_ticket
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    event_type_ref,
    ticket_tags,
    ticket_priority,
    ticket_channel,
    last_event_at,
    event_at_decile,
    last_known_event_at,
    prev_event_at,
    prev3_event_at,
    second_event_at,
    event_at_rank,
    first_event_at,
    event_at_percent_rank,
    next_event_at_2,
    rn,
    CASE WHEN ticket_priority IN ('P1', 'P2') THEN 'group_a' WHEN ticket_priority = 'P4' THEN 'group_b' ELSE 'other' END AS ticket_priority_group
FROM windowed
WHERE ticket_priority = 'P4'
