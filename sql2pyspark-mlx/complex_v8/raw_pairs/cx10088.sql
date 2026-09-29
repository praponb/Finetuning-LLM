WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        CASE WHEN event_type IN ('resolved', 'reopened') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group,
        MONTH(event_at) AS event_at_month
    FROM ticket_events
    WHERE event_type IN ('reopened', 'resolved', 'assigned')
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM recent_events s
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_type_or_unknown,
        event_at_month,
        event_count,
        LAG(event_at) OVER (PARTITION BY event_at_month ORDER BY event_count DESC, id) AS prev_event_at,
        NTH_VALUE(event_at_month, 2) OVER (PARTITION BY event_at_month ORDER BY event_count DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_month,
        MAX(event_count) OVER w_ticket_id AS ticket_id_max_event_count,
        ROUND(CUME_DIST() OVER (PARTITION BY event_at_month ORDER BY event_count DESC), 3) AS event_count_cume_dist
    FROM summarized_rows
    WINDOW w_ticket_id AS (PARTITION BY ticket_id)
),
combined_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_at_month,
        s.prev_event_at,
        s.ticket_id_max_event_count,
        s.event_count_cume_dist,
        t.csat AS ticket_csat,
        t.agent_id AS ticket_agent_id
    FROM windowed s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT id, metric_name, amount
FROM (
    SELECT id, ticket_id_max_event_count, ticket_csat
    FROM combined_details
) src
UNPIVOT (
    amount FOR metric_name IN (ticket_id_max_event_count, ticket_csat)
) unp
