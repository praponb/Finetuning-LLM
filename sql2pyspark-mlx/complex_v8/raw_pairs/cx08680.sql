WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 9 AND 15
      AND ticket_id IS NOT NULL
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.actor_event_type_label,
        t.channel AS ticket_channel
    FROM valid s
    JOIN tickets t ON t.id = s.ticket_id
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.actor_event_type_label,
        t.priority AS ticket_priority,
        t.csat AS ticket_csat
    FROM enriched s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        actor_event_type_label,
        ticket_priority,
        ticket_csat,
        NTH_VALUE(ticket_csat, 2) OVER (PARTITION BY ticket_csat, event_type ORDER BY ticket_csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_csat,
        DENSE_RANK() OVER w_ticket_csat_event_type_by_ti_2 AS ticket_csat_dense_rank,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY actor)) AS distinct_ticket_id_in_actor,
        MAX(ticket_csat) OVER (PARTITION BY ticket_csat, event_type ORDER BY ticket_csat ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS ticket_csat_max_1_prior,
        FIRST_VALUE(ticket_csat) OVER (PARTITION BY ticket_priority ORDER BY ticket_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_csat
    FROM joined_ticket
    WINDOW w_ticket_csat_event_type_by_ti_2 AS (PARTITION BY ticket_csat, event_type ORDER BY ticket_csat ASC)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    actor_event_type_label,
    ticket_priority,
    ticket_csat,
    second_ticket_csat,
    distinct_ticket_id_in_actor,
    ticket_csat_max_1_prior,
    first_ticket_csat,
    CONCAT_WS('|', actor_event_type_label, actor) AS actor_event_type_label_actor_label
FROM sequenced
ORDER BY ticket_csat DESC, event_type DESC NULLS FIRST, id
LIMIT 20
