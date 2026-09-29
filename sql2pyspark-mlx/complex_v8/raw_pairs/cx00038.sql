WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE priority IN ('P2', 'P1', 'P4')
      AND requester_id < 18
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.csat,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor,
        t.event_at
    FROM valid s
    JOIN ticket_events t ON t.ticket_id = s.id
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.event_id,
        s.event_type,
        s.event_at,
        su.hired_on AS agent_hired_on
    FROM exploded_detail s
    JOIN support_agents su ON su.id = s.agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_closed_at,
        (SELECT MAX(t.opened_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_opened_at
    FROM with_parent_agent s
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY agent_id, TO_DATE(opened_at) ORDER BY agent_hired_on, id, event_id) AS agent_hired_on_decile,
        ROUND(CUME_DIST() OVER (ORDER BY last_ticket_opened_at DESC), 3) AS last_ticket_opened_at_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY last_ticket_opened_at DESC, id, event_id), 3) AS last_ticket_opened_at_cume_dist_2,
        ROW_NUMBER() OVER w_agent_id_opened_at_to_date_b AS seq_no
    FROM with_stats
    WINDOW w_agent_id_opened_at_to_date_b AS (PARTITION BY agent_id, TO_DATE(opened_at) ORDER BY agent_hired_on, id, event_id)
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.event_id,
        s.event_type,
        s.event_at,
        s.first_ticket_closed_at,
        s.last_ticket_opened_at,
        s.agent_hired_on_decile,
        s.last_ticket_opened_at_cume_dist,
        s.last_ticket_opened_at_cume_dist_2,
        t.id AS event_id_2,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM sequenced s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
screened AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM with_children s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type = 'escalated'
        )
)
SELECT
    agent_hired_on_decile,
    event_id_2,
    SUM(CASE WHEN event_type IN ('reopened', 'resolved') THEN last_ticket_opened_at_cume_dist ELSE 0 END) AS last_ticket_opened_at_cume_dist_event_type_in_re,
    SUM(CASE WHEN event_actor IN ('customer', 'system') THEN agent_hired_on_decile ELSE 0 END) AS agent_hired_on_decile_event_actor_in_customer_sy,
    ROUND(100 * COUNT(CASE WHEN event_actor = 'agent' THEN 1 END) / COUNT(*), 1) AS pct_event_actor_agent
FROM screened
GROUP BY GROUPING SETS ((agent_hired_on_decile, event_id_2), (agent_hired_on_decile), (event_id_2))
ORDER BY agent_hired_on_decile NULLS LAST, event_id_2 NULLS LAST
