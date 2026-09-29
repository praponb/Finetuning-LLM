WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label,
        CASE WHEN event_type IN ('created', 'escalated') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE actor <> 'agent'
      AND event_type = 'escalated'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref_2
    FROM recent_events
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_label,
        s.event_type_group,
        s.actor_or_unknown,
        s.event_type_ref,
        s.event_type_ref_2,
        t.channel AS ticket_channel,
        t.closed_at AS ticket_closed_at,
        t.csat AS ticket_csat,
        su.hired_on AS agent_hired_on
    FROM scored s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND HOUR(t.opened_at) BETWEEN 7 AND 11
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, MONTH(agent_hired_on) ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER (PARTITION BY actor_or_unknown ORDER BY event_at, id)) AS days_since_prev_agent_hired_on,
        COUNT(*) OVER (PARTITION BY event_type_group) AS rows_in_event_type_group
    FROM enriched_ticket
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM metrics s
)
SELECT DISTINCT id
FROM summarized_rows
ORDER BY id DESC NULLS FIRST
