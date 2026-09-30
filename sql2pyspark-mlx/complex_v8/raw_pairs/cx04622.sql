WITH recent AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('|', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_at >= TIMESTAMP '2025-01-25 09:00:00'
),
combined_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor_event_type_label,
        t.opened_at AS ticket_opened_at,
        t.tags AS ticket_tags
    FROM recent s
    JOIN tickets t ON t.id = s.ticket_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER w_all_by_ticket_opened_at AS ticket_opened_at_quartile,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY TO_DATE(event_at) ORDER BY ticket_opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTILE(5) OVER w_event_at_to_date_by_ticket_o_2 AS ticket_opened_at_quintile,
        RANK() OVER (PARTITION BY event_type ORDER BY event_at DESC) AS event_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(event_at) ORDER BY ticket_opened_at), 3) AS ticket_opened_at_cume_dist,
        RANK() OVER (ORDER BY ticket_opened_at ASC, id) AS ticket_opened_at_rank,
        DENSE_RANK() OVER w_event_type_by_event_at AS event_at_dense_rank
    FROM combined_details
    WINDOW w_all_by_ticket_opened_at AS (ORDER BY ticket_opened_at ASC, id), w_event_at_to_date_by_ticket_o_2 AS (PARTITION BY TO_DATE(event_at) ORDER BY ticket_opened_at, id), w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at DESC, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        actor_event_type_label,
        ticket_tags,
        ticket_opened_at_quintile,
        event_at_rank,
        event_at_dense_rank,
        FLOOR(ticket_opened_at_rank / 2) * 2 AS ticket_opened_at_rank_bucket,
        DATE_TRUNC('WEEK', second_event_at) AS second_event_at_week,
        ROUND((UNIX_TIMESTAMP(second_event_at) - UNIX_TIMESTAMP(ticket_opened_at)) / 3600, 1) AS hours_ticket_opened_at_to_second_event_at
    FROM with_windows
)
-- Final projection
SELECT
    id,
    actor_event_type_label,
    ticket_opened_at_quintile,
    event_at_rank,
    event_at_dense_rank,
    ticket_opened_at_rank_bucket,
    hours_ticket_opened_at_to_second_event_at,
    ABS(event_at_dense_rank - 7) AS event_at_dense_rank_distance
FROM enriched
ORDER BY actor_event_type_label DESC, id
LIMIT 10
