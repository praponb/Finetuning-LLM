WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('-', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE event_type IN ('resolved', 'replied')
      AND event_at < TIMESTAMP '2025-03-02 09:00:00'
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor_event_type_label,
        t.channel AS ticket_channel,
        t.closed_at AS ticket_closed_at,
        t.tags AS ticket_tags,
        su.team AS agent_team
    FROM prepared_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY MONTH(ticket_closed_at) ORDER BY ticket_closed_at, id) AS rn,
        FIRST_VALUE(ticket_closed_at) IGNORE NULLS OVER (ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_closed_at,
        ROW_NUMBER() OVER (ORDER BY ticket_closed_at, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY MONTH(ticket_closed_at) ORDER BY ticket_closed_at) AS ticket_closed_at_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY ticket_closed_at ASC, id), 3) AS ticket_closed_at_cume_dist,
        RANK() OVER (PARTITION BY ticket_channel ORDER BY ticket_closed_at) AS ticket_closed_at_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_ticket_closed_at, 3) AS ticket_closed_at_percent_rank,
        LAG(ticket_closed_at) OVER (PARTITION BY ticket_channel ORDER BY ticket_closed_at, id) AS prev_ticket_closed_at,
        NTH_VALUE(ticket_closed_at, 2) OVER (ORDER BY ticket_closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at,
        NTH_VALUE(ticket_closed_at, 2) OVER (PARTITION BY ticket_id ORDER BY ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at_2,
        NTH_VALUE(ticket_closed_at, 2) OVER (PARTITION BY MONTH(ticket_closed_at) ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at_3,
        LAST_VALUE(ticket_closed_at) IGNORE NULLS OVER (PARTITION BY MONTH(ticket_closed_at) ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_closed_at,
        DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY ticket_closed_at DESC, id) AS ticket_closed_at_dense_rank_2,
        ROUND(CUME_DIST() OVER w_ticket_closed_at_month_by_ti, 3) AS ticket_closed_at_cume_dist_2,
        LAG(ticket_closed_at) OVER w_ticket_id_by_ticket_closed_a_2 AS prev_ticket_closed_at_2
    FROM with_details
    WINDOW w_all_by_ticket_closed_at AS (ORDER BY ticket_closed_at ASC, id), w_ticket_closed_at_month_by_ti AS (PARTITION BY MONTH(ticket_closed_at) ORDER BY ticket_closed_at, id), w_ticket_id_by_ticket_closed_a_2 AS (PARTITION BY ticket_id ORDER BY ticket_closed_at DESC, id)
)
-- Final projection
SELECT
    id,
    event_type,
    actor_event_type_label,
    ticket_channel,
    ticket_closed_at,
    first_known_ticket_closed_at,
    seq_no,
    ticket_closed_at_dense_rank,
    ticket_closed_at_rank,
    ticket_closed_at_percent_rank,
    prev_ticket_closed_at,
    second_ticket_closed_at,
    second_ticket_closed_at_2,
    second_ticket_closed_at_3,
    last_known_ticket_closed_at,
    ticket_closed_at_dense_rank_2,
    ticket_closed_at_cume_dist_2,
    prev_ticket_closed_at_2,
    ABS(ticket_closed_at_rank - 5) AS ticket_closed_at_rank_distance
FROM sequenced
WHERE second_ticket_closed_at >= TIMESTAMP '2025-01-24 01:00:00'
