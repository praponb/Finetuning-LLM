WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE event_type NOT IN ('escalated', 'reopened')
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        t.requester_id AS ticket_requester_id
    FROM filtered_events s
    JOIN tickets t ON t.id = s.ticket_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        ticket_requester_id,
        SUM(ticket_requester_id) OVER w_ticket_id AS ticket_id_sum_ticket_requester_id,
        MIN(ticket_requester_id) OVER (PARTITION BY ticket_id) AS ticket_id_min_ticket_requester_id,
        ROUND(CUME_DIST() OVER (ORDER BY ticket_requester_id DESC), 3) AS ticket_requester_id_cume_dist,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER (PARTITION BY ticket_id), 0), 4) AS ticket_requester_id_share_of_ticket_id,
        LAG(ticket_requester_id, 2, 0) OVER w_all_by_ticket_requester_id AS prev2_ticket_requester_id,
        NTH_VALUE(event_at, 2) OVER (ORDER BY ticket_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAST_VALUE(ticket_requester_id) IGNORE NULLS OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_requester_id,
        LAG(event_at) OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id) AS prev_event_at,
        MAX(ticket_requester_id) OVER (PARTITION BY ticket_id) AS ticket_id_max_ticket_requester_id,
        RANK() OVER (PARTITION BY actor ORDER BY ticket_requester_id) AS ticket_requester_id_rank,
        LAST_VALUE(ticket_requester_id) IGNORE NULLS OVER (PARTITION BY actor ORDER BY ticket_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_requester_id_2
    FROM combined_details_ticket
    WINDOW w_ticket_id AS (PARTITION BY ticket_id), w_all_by_ticket_requester_id AS (ORDER BY ticket_requester_id DESC, id)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        ticket_id_sum_ticket_requester_id,
        ticket_requester_id_cume_dist,
        ticket_requester_id_share_of_ticket_id,
        prev2_ticket_requester_id,
        second_event_at,
        last_known_ticket_requester_id,
        prev_event_at,
        ticket_requester_id_rank,
        last_known_ticket_requester_id_2,
        CASE WHEN ticket_requester_id >= 17 THEN 'A' WHEN ticket_requester_id >= 8 THEN 'B' ELSE 'C' END AS ticket_requester_id_tier
    FROM sequenced
    WHERE ticket_requester_id_share_of_ticket_id >= 0.5
)
SELECT
    id,
    event_at,
    ticket_id_sum_ticket_requester_id,
    ticket_requester_id_cume_dist,
    prev2_ticket_requester_id,
    second_event_at,
    last_known_ticket_requester_id,
    last_known_ticket_requester_id_2,
    ticket_requester_id_tier,
    ABS(ticket_requester_id_rank - 7) AS ticket_requester_id_rank_distance
FROM labeled
WHERE ticket_requester_id_share_of_ticket_id <= 0.6
ORDER BY last_known_ticket_requester_id DESC NULLS FIRST, prev2_ticket_requester_id
