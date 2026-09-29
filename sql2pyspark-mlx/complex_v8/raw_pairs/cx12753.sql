WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'login')
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        t.id AS event_id,
        t.event_type,
        t.event_at,
        t.actor AS event_actor
    FROM valid s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id AND t.event_at >= TIMESTAMP '2025-02-06 10:00:00'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.event_id,
        s.event_at,
        s.event_actor,
        t.event_at AS event_event_at
    FROM line_level_events s
    JOIN ticket_events t ON t.id = s.event_id
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', event_event_at) AS days_since_event_event_at
    FROM combined_details
    WHERE event_at >= TIMESTAMP '2025-01-25 14:00:00'
),
event_actor_stats AS (
    -- Avg requester_id per event_actor
    SELECT event_actor, AVG(requester_id) AS avg_requester_id
    FROM labeled
    GROUP BY event_actor
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM labeled s
    JOIN event_actor_stats g ON g.event_actor = s.event_actor
    WHERE s.requester_id > g.avg_requester_id
),
totals AS (
    -- Summarise per group
    SELECT
        agent_id,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        COUNT(*) FILTER (WHERE event_actor = 'customer') AS n_event_actor_customer,
        ARRAY_SORT(COLLECT_SET(event_actor)) AS event_actor_list,
        MIN(opened_at) AS first_opened_at,
        COUNT(*) AS n_rows,
        MIN(avg_requester_id) AS min_avg_requester_id
    FROM outliers
    GROUP BY 1, 2
),
with_details_agent AS (
    -- Join support_agents
    SELECT
        s.agent_id,
        s.event_at_month,
        s.n_event_actor_customer,
        s.n_rows,
        s.min_avg_requester_id,
        su.team AS agent_team
    FROM totals s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    agent_id,
    event_at_month,
    n_event_actor_customer,
    n_rows,
    agent_team
FROM with_details_agent
WHERE MONTH(event_at_month) IN (2, 3)
ORDER BY event_at_month
