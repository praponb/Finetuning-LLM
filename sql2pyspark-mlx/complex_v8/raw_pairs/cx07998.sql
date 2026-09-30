WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('YEAR', event_at) AS event_at_year,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM ticket_events
    WHERE (event_at < TIMESTAMP '2025-02-12 09:00:00' OR event_type = 'escalated')
),
joined AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        t.priority AS ticket_priority,
        t.agent_id AS ticket_agent_id,
        t.csat AS ticket_csat
    FROM recent_events s
    JOIN tickets t ON t.id = s.ticket_id
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        ticket_agent_id,
        ticket_id,
        SUM(CASE WHEN ticket_priority = 'P2' THEN 1 ELSE 0 END) AS n_ticket_priority_p2,
        ROUND(STDDEV(ticket_csat), 2) AS stddev_ticket_csat,
        ARRAY_SORT(COLLECT_SET(ticket_priority)) AS ticket_priority_list,
        MIN(ticket_csat) AS min_ticket_csat,
        SUM(ticket_csat) AS total_ticket_csat
    FROM joined
    GROUP BY DATE_TRUNC('MONTH', event_at), ticket_agent_id, ticket_id
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.event_at_month,
        s.ticket_agent_id,
        s.ticket_id,
        s.n_ticket_priority_p2,
        s.stddev_ticket_csat,
        s.ticket_priority_list,
        s.min_ticket_csat,
        s.total_ticket_csat,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM totals s
    JOIN support_agents su ON su.id = s.ticket_agent_id
)
SELECT
    event_at_month,
    ticket_agent_id,
    ticket_id,
    n_ticket_priority_p2,
    stddev_ticket_csat,
    min_ticket_csat,
    total_ticket_csat,
    agent_team,
    agent_hired_on,
    agent_tier,
    CASE WHEN stddev_ticket_csat >= 3.2 THEN 'A' WHEN stddev_ticket_csat >= 2.2 THEN 'B' ELSE 'C' END AS stddev_ticket_csat_tier
FROM with_parent_agent
WHERE ticket_id IS NOT NULL
ORDER BY agent_hired_on
