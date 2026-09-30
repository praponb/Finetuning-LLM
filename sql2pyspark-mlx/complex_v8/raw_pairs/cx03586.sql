WITH matched AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        MAX(csat) AS max_ticket_csat,
        MIN(requester_id) AS min_ticket_requester_id
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.max_ticket_csat,
        t.min_ticket_requester_id
    FROM matched s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM summarized_rows s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        agent_id,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        ROUND(100 * COUNT(CASE WHEN event_actor <> 'customer' THEN 1 END) / COUNT(*), 1) AS pct_event_actor_customer,
        COUNT(*) FILTER (WHERE event_event_type IN ('escalated', 'created', 'resolved')) AS n_event_event_type_in_escalate,
        SUM(CASE WHEN priority <> 'P2' THEN 1 ELSE 0 END) AS n_priority_p2,
        COUNT(*) FILTER (WHERE event_event_type NOT IN ('created', 'resolved')) AS n_event_event_type_not_in_crea,
        ROUND(100 * COUNT(CASE WHEN priority <> 'P4' THEN 1 END) / COUNT(*), 1) AS pct_priority_p4
    FROM with_children_events
    GROUP BY agent_id, DATE_TRUNC('MONTH', event_at)
)
-- Final projection
SELECT
    agent_id,
    distinct_agent_ids,
    pct_event_actor_customer,
    n_event_event_type_in_escalate,
    n_priority_p2,
    n_event_event_type_not_in_crea,
    DATE_TRUNC('MONTH', event_at_month) AS event_at_month_month,
    DATEDIFF(DATE '2025-12-31', event_at_month) AS days_since_event_at_month
FROM rollup_base
WHERE n_event_event_type_in_escalate >= 2
ORDER BY agent_id DESC, days_since_event_at_month DESC NULLS FIRST
