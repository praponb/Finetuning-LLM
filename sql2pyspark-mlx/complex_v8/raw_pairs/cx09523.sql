WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        CASE WHEN event_type IN ('resolved', 'created') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group,
        CASE WHEN actor IN ('system', 'agent') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE (event_type <> 'assigned' OR event_at >= TIMESTAMP '2025-01-31 13:00:00')
),
joined_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.requester_id AS ticket_requester_id,
        t.channel AS ticket_channel,
        t.opened_at AS ticket_opened_at
    FROM filtered s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(ticket_requester_id) OVER (PARTITION BY event_type_group) AS event_type_group_max_ticket_requester_id,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_type_group)) AS distinct_actor_in_event_type_group,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER w_event_type_group, 2) AS ticket_requester_id_vs_event_type_group_avg,
        ROUND(AVG(ticket_requester_id) OVER (PARTITION BY ticket_id ORDER BY ticket_requester_id, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ticket_requester_id_avg_4
    FROM joined_ticket
    WINDOW w_event_type_group AS (PARTITION BY event_type_group)
),
combined_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_at_label,
        s.actor_group,
        s.ticket_channel,
        s.ticket_opened_at,
        s.event_type_group_max_ticket_requester_id,
        s.distinct_actor_in_event_type_group,
        s.ticket_requester_id_vs_event_type_group_avg,
        s.ticket_requester_id_avg_4,
        t.closed_at AS ticket_closed_at
    FROM with_trends s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.closed_at < TIMESTAMP '2025-03-09 04:00:00'
)
SELECT id, metric, amount
FROM (
    SELECT id, ticket_requester_id_vs_event_type_group_avg, ticket_requester_id_avg_4
    FROM combined_details
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric IN (ticket_requester_id_vs_event_type_group_avg AS ticket_requester_id_vs_event_type_group_avg_v, ticket_requester_id_avg_4 AS ticket_requester_id_avg_4_v)
) unp
ORDER BY id, metric
