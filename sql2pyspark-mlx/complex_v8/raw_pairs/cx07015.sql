WITH prepared_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tickets
    WHERE requester_id >= 7
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM prepared_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type IS NOT NULL
        )
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        csat - LAG(csat) OVER (ORDER BY requester_id DESC NULLS FIRST, id) AS csat_change,
        SIZE(COLLECT_SET(priority) OVER w_agent_id) AS distinct_priority_in_agent_id,
        ROUND(requester_id - AVG(requester_id) OVER w_agent_id, 2) AS requester_id_vs_agent_id_avg,
        NTILE(5) OVER (ORDER BY requester_id DESC NULLS FIRST, id) AS requester_id_quintile,
        ROUND(csat - AVG(csat) OVER (PARTITION BY agent_id), 2) AS csat_vs_agent_id_avg
    FROM qualified
    WINDOW w_agent_id AS (PARTITION BY agent_id)
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    priority,
    csat,
    tags,
    csat_change,
    distinct_priority_in_agent_id,
    requester_id_vs_agent_id_avg,
    requester_id_quintile,
    CASE WHEN requester_id >= 20 THEN 'large' WHEN requester_id >= 9 THEN 'medium' ELSE 'small' END AS requester_id_tier
FROM windowed
ORDER BY requester_id NULLS LAST
