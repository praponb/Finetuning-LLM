WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM ticket_events
),
with_parent_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        t.agent_id AS ticket_agent_id,
        t.priority AS ticket_priority,
        t.csat AS ticket_csat
    FROM filtered_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.agent_id <= 2
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(ticket_csat) OVER (PARTITION BY actor ORDER BY ticket_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ticket_csat_so_far,
        MIN(ticket_csat) OVER (PARTITION BY ticket_agent_id) AS ticket_agent_id_min_ticket_csat,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at), 3) AS event_at_percent_rank,
        ROUND(PERCENT_RANK() OVER w_actor_by_ticket_csat_ties, 3) AS ticket_csat_percent_rank,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_priority, ticket_agent_id ORDER BY ticket_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROUND(ticket_csat - AVG(ticket_csat) OVER (PARTITION BY ticket_agent_id), 2) AS ticket_csat_vs_ticket_agent_id_avg,
        COUNT(*) OVER (PARTITION BY ticket_agent_id) AS rows_in_ticket_agent_id,
        LEAD(event_at) OVER w_all_by_event_at AS next_event_at,
        LEAD(ticket_csat) OVER (PARTITION BY actor ORDER BY ticket_csat DESC, id) AS next_ticket_csat,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY ticket_agent_id)) AS distinct_ticket_id_in_ticket_agent_id,
        SIZE(COLLECT_SET(ticket_priority) OVER (PARTITION BY ticket_agent_id)) AS distinct_ticket_priority_in_ticket_agent_id,
        ROW_NUMBER() OVER w_actor_by_ticket_csat AS seq_no,
        MAX(ticket_csat) OVER (PARTITION BY ticket_priority, ticket_agent_id ORDER BY ticket_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ticket_csat_so_far
    FROM with_parent_ticket
    WINDOW w_actor_by_ticket_csat_ties AS (PARTITION BY actor ORDER BY ticket_csat DESC), w_all_by_event_at AS (ORDER BY event_at, id), w_actor_by_ticket_csat AS (PARTITION BY actor ORDER BY ticket_csat DESC, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    ticket_priority,
    ticket_csat,
    min_ticket_csat_so_far,
    ticket_agent_id_min_ticket_csat,
    event_at_percent_rank,
    last_known_event_at,
    rows_in_ticket_agent_id,
    next_event_at,
    next_ticket_csat,
    distinct_ticket_priority_in_ticket_agent_id,
    seq_no,
    max_ticket_csat_so_far,
    CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group,
    TO_DATE(next_event_at) AS next_event_at_date
FROM windowed
ORDER BY rows_in_ticket_agent_id DESC, seq_no DESC, id
LIMIT 20
