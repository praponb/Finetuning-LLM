WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix,
        CASE WHEN event_type IN ('reopened', 'escalated') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM cleaned s
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.actor_prefix,
        s.event_type_group,
        t.tags AS ticket_tags,
        t.closed_at AS ticket_closed_at,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM with_counts s
    JOIN tickets t ON t.id = s.ticket_id AND t.requester_id < 23
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        MONTH(agent_hired_on) AS agent_hired_on_month,
        DATE_FORMAT(ticket_closed_at, 'yyyy-MM') AS ticket_closed_at_label
    FROM with_details_ticket
),
exploded AS (
    -- One row per ticket_tag
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.actor_prefix,
        s.event_type_group,
        s.ticket_closed_at,
        s.agent_name,
        s.agent_hired_on,
        s.agent_hired_on_month,
        s.ticket_closed_at_label,
        t.ticket_tag
    FROM scored s
    LATERAL VIEW EXPLODE(s.ticket_tags) t AS ticket_tag
),
event_type_stats AS (
    -- Max agent_hired_on_month per event_type
    SELECT event_type, MAX(agent_hired_on_month) AS max_agent_hired_on_month
    FROM exploded
    GROUP BY event_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_agent_hired_on_month,
        ROUND(s.agent_hired_on_month - g.max_agent_hired_on_month, 2) AS agent_hired_on_month_minus_max
    FROM exploded s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.agent_hired_on_month = g.max_agent_hired_on_month
)
-- Final projection
SELECT
    id,
    actor,
    actor_prefix,
    event_type_group,
    agent_name,
    agent_hired_on,
    agent_hired_on_month,
    ticket_closed_at_label,
    ticket_tag,
    max_agent_hired_on_month,
    agent_hired_on_month_minus_max,
    YEAR(ticket_closed_at) AS ticket_closed_at_year,
    COALESCE(agent_hired_on_month, 0) AS agent_hired_on_month_filled
FROM vs_group
ORDER BY agent_hired_on_month_filled NULLS LAST
