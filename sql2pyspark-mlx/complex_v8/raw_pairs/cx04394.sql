WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('agent', 'customer') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group,
        event_at + INTERVAL 1 DAYS AS event_at_plus_1d
    FROM ticket_events
    WHERE (ticket_id IS NOT NULL OR actor = 'customer')
),
with_parent AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.actor_group,
        s.event_at_plus_1d,
        t.requester_id AS ticket_requester_id,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM scoped s
    JOIN tickets t ON t.id = s.ticket_id AND t.channel = 'web'
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
event_type_stats AS (
    -- Max ticket_requester_id per event_type
    SELECT event_type, MAX(ticket_requester_id) AS max_ticket_requester_id
    FROM with_parent
    GROUP BY event_type
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ticket_requester_id,
        ROUND(s.ticket_requester_id - g.max_ticket_requester_id, 2) AS ticket_requester_id_minus_max
    FROM with_parent s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.ticket_requester_id = g.max_ticket_requester_id
),
latest AS (
    -- Keep the earliest row per actor_group, actor
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        actor_group,
        event_at_plus_1d,
        ticket_requester_id,
        agent_name,
        agent_hired_on,
        max_ticket_requester_id,
        ticket_requester_id_minus_max
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor_group, actor ORDER BY ticket_requester_id_minus_max, id) AS row_num
        FROM compared
    ) ranked
    WHERE row_num = 1
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        MAX(event_at) AS last_event_at,
        COUNT(CASE WHEN actor = 'system' THEN 1 END) AS event_actor_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        t.last_event_at,
        COALESCE(t.event_actor_count, 0) AS event_actor_count
    FROM latest s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
unique_rows AS (
    -- Keep the latest row per ticket_id, event_type
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        actor_group,
        event_at_plus_1d,
        ticket_requester_id,
        agent_name,
        agent_hired_on,
        max_ticket_requester_id,
        ticket_requester_id_minus_max,
        first_event_at,
        last_event_at,
        event_actor_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY max_ticket_requester_id DESC, id DESC) AS row_num
        FROM summarized_rows
    ) ranked
    WHERE row_num = 1
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', first_event_at) AS first_event_at_month,
        COUNT(*) FILTER (WHERE event_type <> 'resolved') AS n_event_type_resolved,
        SUM(ticket_requester_id) AS total_ticket_requester_id,
        SUM(event_actor_count) FILTER (WHERE UPPER(TRIM(agent_name)) LIKE 'J%') AS event_actor_count_upper_trim_agent_name_like_j
    FROM unique_rows
    WHERE ticket_requester_id < 22
    GROUP BY DATE_TRUNC('MONTH', first_event_at)
)
-- Final projection
SELECT
    first_event_at_month,
    total_ticket_requester_id,
    event_actor_count_upper_trim_agent_name_like_j
FROM stats
WHERE n_event_type_resolved <= 7
ORDER BY total_ticket_requester_id NULLS LAST
