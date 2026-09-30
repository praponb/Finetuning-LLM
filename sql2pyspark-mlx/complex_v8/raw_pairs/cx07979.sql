WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE event_type NOT IN ('resolved', 'created')
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.days_since_event_at,
        t.channel AS ticket_channel,
        t.opened_at AS ticket_opened_at
    FROM cleaned s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(days_since_event_at) OVER (PARTITION BY ticket_channel ORDER BY days_since_event_at DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS days_since_event_at_max_1_prior,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER (PARTITION BY event_type, actor), 0), 4) AS days_since_event_at_share_of_event_type_actor,
        FIRST_VALUE(ticket_opened_at) OVER (PARTITION BY ticket_channel ORDER BY days_since_event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_opened_at,
        ROUND(AVG(days_since_event_at) OVER (PARTITION BY event_type, actor), 2) AS event_type_actor_avg_days_since_event_at,
        RANK() OVER w_ticket_channel_by_days_since_3 AS days_since_event_at_rank,
        MAX(days_since_event_at) OVER (PARTITION BY event_type, actor) AS event_type_actor_max_days_since_event_at
    FROM with_details_ticket
    WINDOW w_ticket_channel_by_days_since_3 AS (PARTITION BY ticket_channel ORDER BY days_since_event_at DESC NULLS FIRST)
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at
    FROM metrics s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
SELECT
    COALESCE(ticket_channel, 'ALL') AS ticket_channel,
    ticket_id,
    days_since_event_at_rank,
    MIN(first_event_at) AS first_first_event_at,
    SUM(event_type_actor_max_days_since_event_at) AS total_event_type_actor_max_days_since_event_at
FROM with_stats
GROUP BY CUBE(ticket_channel, ticket_id, days_since_event_at_rank)
ORDER BY ticket_channel NULLS LAST, ticket_id NULLS LAST, days_since_event_at_rank NULLS LAST
