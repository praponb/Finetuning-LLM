WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at,
        HOUR(closed_at) AS closed_at_hour
    FROM tickets
    WHERE priority IN ('P2', 'P4', 'P1')
      AND TO_DATE(closed_at) < DATE '2025-02-26'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        priority,
        csat,
        closed_at_hour,
        ROUND(CUME_DIST() OVER w_all_by_requester_id_ties, 3) AS requester_id_cume_dist,
        ROUND(PERCENT_RANK() OVER w_all_by_requester_id_ties, 3) AS requester_id_percent_rank,
        SUM(closed_at_hour) OVER (PARTITION BY agent_id) AS agent_id_sum_closed_at_hour,
        ROW_NUMBER() OVER (ORDER BY requester_id DESC NULLS FIRST, id) AS seq_no
    FROM recent
    WINDOW w_all_by_requester_id_ties AS (ORDER BY requester_id NULLS LAST)
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.priority,
        s.csat,
        s.agent_id_sum_closed_at_hour,
        s.seq_no,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor,
        t.event_at
    FROM windowed s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        csat,
        agent_id_sum_closed_at_hour,
        seq_no,
        event_id,
        event_type,
        event_actor,
        CONCAT_WS('-', event_actor, event_type) AS event_actor_event_type_label,
        CONCAT_WS(' / ', event_type, event_actor) AS event_type_event_actor_label
    FROM line_level_events
)
-- Final projection
SELECT
    id,
    agent_id_sum_closed_at_hour,
    event_id,
    event_type,
    event_actor_event_type_label,
    event_type_event_actor_label
FROM enriched
ORDER BY event_type NULLS LAST, agent_id_sum_closed_at_hour DESC, id, event_id
LIMIT 5
