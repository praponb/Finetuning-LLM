WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len
    FROM ticket_events
    WHERE actor = 'system'
      AND event_at < TIMESTAMP '2025-02-24 08:00:00'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(actor_len) OVER w_actor_event_type AS actor_event_type_min_actor_len,
        LEAD(actor_len, 1, 0) OVER (ORDER BY actor_len, id) AS next_actor_len,
        RANK() OVER (ORDER BY actor_len) AS actor_len_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY actor_len), 3) AS actor_len_percent_rank,
        ROUND(actor_len - AVG(actor_len) OVER (PARTITION BY actor, event_type), 2) AS actor_len_vs_actor_event_type_avg
    FROM cleaned_events
    WINDOW w_actor_event_type AS (PARTITION BY actor, event_type)
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.actor_len,
        s.next_actor_len,
        s.actor_len_rank,
        s.actor_len_percent_rank,
        s.actor_len_vs_actor_event_type_avg,
        t.tags AS ticket_tags,
        t.priority AS ticket_priority,
        t.channel AS ticket_channel
    FROM with_trends s
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    actor,
    actor_len,
    next_actor_len,
    actor_len_rank,
    actor_len_vs_actor_event_type_avg,
    ticket_tags,
    ticket_priority,
    LENGTH(TRIM(ticket_priority)) AS ticket_priority_len,
    ABS(actor_len - 10) AS actor_len_distance
FROM joined
WHERE SIZE(ticket_tags) >= 2
ORDER BY actor_len DESC
