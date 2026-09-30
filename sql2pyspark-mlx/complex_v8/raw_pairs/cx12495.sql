WITH cleaned AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 10 AND 14
      AND event_type = 'escalated'
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.closed_at AS ticket_closed_at,
        t.priority AS ticket_priority,
        t.requester_id AS ticket_requester_id
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (ORDER BY ticket_requester_id DESC, id) AS ticket_requester_id_rank,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY ticket_priority)) AS distinct_event_type_in_ticket_priority,
        SUM(ticket_requester_id) OVER (ORDER BY event_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ticket_requester_id_sum_6_prior,
        ROUND(CUME_DIST() OVER (ORDER BY event_at, id), 3) AS event_at_cume_dist,
        MAX(ticket_requester_id) OVER w_ticket_priority AS ticket_priority_max_ticket_requester_id,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY ticket_priority), 2) AS ticket_requester_id_vs_ticket_priority_avg,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY ticket_id), 2) AS ticket_requester_id_vs_ticket_id_avg,
        LAST_VALUE(ticket_closed_at) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY ticket_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_closed_at,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER w_ticket_id_actor, 2) AS ticket_requester_id_vs_ticket_id_actor_avg,
        ROUND(AVG(ticket_requester_id) OVER (PARTITION BY ticket_priority), 2) AS ticket_priority_avg_ticket_requester_id,
        RANK() OVER w_event_at_month_ticket_priori AS ticket_closed_at_rank,
        ROW_NUMBER() OVER w_all_by_event_at AS event_at_seq,
        NTH_VALUE(ticket_closed_at, 2) OVER (PARTITION BY ticket_id ORDER BY ticket_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY ticket_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY MONTH(event_at), ticket_priority ORDER BY ticket_closed_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at
    FROM with_details_ticket
    WINDOW w_ticket_priority AS (PARTITION BY ticket_priority), w_ticket_id_actor AS (PARTITION BY ticket_id, actor), w_event_at_month_ticket_priori AS (PARTITION BY MONTH(event_at), ticket_priority ORDER BY ticket_closed_at NULLS LAST, id), w_all_by_event_at AS (ORDER BY event_at, id)
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        ticket_closed_at,
        ticket_priority,
        ticket_requester_id,
        ticket_requester_id_rank,
        distinct_event_type_in_ticket_priority,
        ticket_requester_id_sum_6_prior,
        event_at_cume_dist,
        ticket_priority_max_ticket_requester_id,
        ticket_requester_id_vs_ticket_priority_avg,
        ticket_requester_id_vs_ticket_id_avg,
        last_known_ticket_closed_at,
        ticket_requester_id_vs_ticket_id_actor_avg,
        ticket_priority_avg_ticket_requester_id,
        ticket_closed_at_rank,
        second_ticket_closed_at,
        second_event_at,
        first_known_event_at
    FROM windowed
    WHERE event_at_seq <= 2
      AND ticket_priority_avg_ticket_requester_id BETWEEN 7.0 AND 19.0
)
SELECT
    id,
    event_at,
    event_type,
    actor,
    ticket_closed_at,
    ticket_priority,
    ticket_requester_id,
    ticket_requester_id_sum_6_prior,
    event_at_cume_dist,
    ticket_priority_max_ticket_requester_id,
    ticket_requester_id_vs_ticket_priority_avg,
    ticket_requester_id_vs_ticket_id_avg,
    last_known_ticket_closed_at,
    ticket_priority_avg_ticket_requester_id,
    ticket_closed_at_rank,
    second_event_at,
    first_known_event_at,
    UPPER(TRIM(ticket_priority)) AS ticket_priority_clean
FROM top_per_group
WHERE event_type = 'escalated'
ORDER BY last_known_ticket_closed_at
